from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, cast
from unittest import mock
from urllib.parse import urljoin

import pytest
from packaging.version import Version

import scrapy
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.extensions.feedexport import (
    FeedExporter,
    FeedSlot,
    S3FeedStorage,
    _FeedManifest,
    _local_path,
)
from scrapy.utils.misc import build_from_crawler
from scrapy.utils.test import get_crawler
from tests.spiders import ItemSpider
from tests.utils.decorators import coroutine_test
from tests.utils.feedexport import PARSERS, MyItem, crawl_items, csv_header, unique_path

if TYPE_CHECKING:
    from collections.abc import Iterable
    from os import PathLike

    from tests.mockserver.http import MockServer


def build_url(path: str | PathLike[str]) -> str:
    path_str = str(path)
    if path_str[0] != "/":
        path_str = "/" + path_str
    return urljoin("file:", path_str)


class MemoryFeedStorage:
    """Keep feeds in memory under a URI scheme that is not the local
    filesystem, so that tests can resume feeds whose targets cannot be
    inspected or removed on disk."""

    supported_modes = frozenset({"append", "create", "overwrite"})
    targets: dict[str, bytes] = {}

    def __init__(self, uri: str, *, feed_options: dict[str, Any] | None = None):
        self.uri = uri
        self.write_mode = (feed_options or {}).get("mode", "append")

    def open(self, spider: scrapy.Spider) -> IO[bytes]:
        return BytesIO()

    def store(self, file: IO[bytes]) -> None:
        data = cast("BytesIO", file).getvalue()
        file.close()
        if self.write_mode == "create" and self.uri in self.targets:
            raise FileExistsError(self.uri)
        if self.write_mode == "append":
            self.targets[self.uri] = self.targets.get(self.uri, b"") + data
        else:
            self.targets[self.uri] = data


async def export_batches(
    mockserver: MockServer, items: Iterable[Any], settings: dict[str, Any]
) -> dict[str, list[bytes]]:
    """Export *items* with *settings*, whose ``FEEDS`` are keyed by local
    batch paths with a separate parent directory for each feed, and return
    the contents of the batch files of each feed format, in file name order."""
    feeds = settings["FEEDS"]
    feed_urls = {build_url(path): options for path, options in feeds.items()}
    await crawl_items(mockserver, items, {**settings, "FEEDS": feed_urls})
    return {
        options["format"]: [
            file.read_bytes() for file in sorted(Path(path).parent.iterdir())
        ]
        if Path(path).parent.exists()
        else []
        for path, options in feeds.items()
    }


class TestBatchDeliveries:
    _file_mark = "_%(batch_time)s_%(batch_id)02d_"

    @pytest.mark.parametrize("fmt", list(PARSERS))
    @coroutine_test
    async def test_export_items(
        self, fmt: str, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Test partial deliveries in all supported formats"""
        items = [
            MyItem({"foo": "bar1", "egg": "spam1"}),
            MyItem({"foo": "bar2", "egg": "spam2", "baz": "quux2"}),
            MyItem({"foo": "bar3", "baz": "quux3"}),
        ]
        settings = {
            "FEEDS": {unique_path(tmp_path) / self._file_mark: {"format": fmt}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        batches = (await export_batches(mockserver, items, settings))[fmt]
        if fmt == "csv":
            header = list(MyItem.fields)
            assert [csv_header(batch) for batch in batches] == [header, header]
        assert [PARSERS[fmt](batch) for batch in batches] == [
            [
                {"egg": "spam1", "foo": "bar1"},
                {"egg": "spam2", "foo": "bar2", "baz": "quux2"},
            ],
            [{"foo": "bar3", "baz": "quux3"}],
        ]

    @coroutine_test
    async def test_batch_delivered_when_full(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Full batches must be finalized and delivered as soon as they are
        full, instead of when the spider closes."""
        dir_path = unique_path(tmp_path)
        batch1_path = Path(dir_path, "1.json")
        mockserver_url = mockserver.url("/")
        batch1_contents: list[bytes | None] = []

        class TestSpider(scrapy.Spider):
            name = "testspider"
            start_urls = [mockserver_url]

            def parse(self, response):
                yield {"foo": "bar1"}
                yield {"foo": "bar2"}
                yield scrapy.Request(
                    mockserver_url, callback=self.parse2, dont_filter=True
                )

            def parse2(self, response):
                # the first batch was full after the second item, so it must
                # have been delivered by now
                batch1_contents.append(
                    batch1_path.read_bytes() if batch1_path.exists() else None
                )
                yield {"foo": "bar3"}

        settings = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.json"): {"format": "json"},
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
        }
        crawler = get_crawler(TestSpider, settings)
        await crawler.crawl_async()

        assert batch1_contents, "the second request was not processed"
        assert batch1_contents[0] is not None, "batch 1 was not stored during the crawl"
        assert json.loads(batch1_contents[0]) == [{"foo": "bar1"}, {"foo": "bar2"}]

    def test_wrong_path(self, tmp_path: Path) -> None:
        """If path is without %(batch_time)s and %(batch_id) an exception must be raised"""
        settings = {
            "FEEDS": {
                unique_path(tmp_path): {"format": "xml"},
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        }
        crawler = get_crawler(settings_dict=settings)
        with pytest.raises(NotConfigured):
            build_from_crawler(FeedExporter, crawler)

    @pytest.mark.parametrize("fmt", ["json", "jsonlines", "xml", "csv"])
    @coroutine_test
    async def test_export_no_items_not_store_empty(
        self, fmt: str, mockserver: MockServer, tmp_path: Path
    ) -> None:
        settings = {
            "FEEDS": {
                unique_path(tmp_path) / self._file_mark: {"format": fmt},
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "FEED_STORE_EMPTY": False,
        }
        data = await export_batches(mockserver, [], settings)
        assert len(data[fmt]) == 0

    @pytest.mark.parametrize(
        ("fmt", "expected"),
        [
            pytest.param("json", b"[]", id="json"),
            pytest.param("jsonlines", b"", id="jsonlines"),
            pytest.param(
                "xml",
                b'<?xml version="1.0" encoding="utf-8"?>\n<items></items>',
                id="xml",
            ),
            pytest.param("csv", b"", id="csv"),
        ],
    )
    @coroutine_test
    async def test_export_no_items_store_empty(
        self, fmt: str, expected: bytes, mockserver: MockServer, tmp_path: Path
    ) -> None:
        settings = {
            "FEEDS": {
                unique_path(tmp_path) / self._file_mark: {"format": fmt},
            },
            "FEED_STORE_EMPTY": True,
            "FEED_EXPORT_INDENT": None,
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        }
        data = await export_batches(mockserver, [], settings)
        assert data[fmt][0] == expected

    @coroutine_test
    async def test_export_multiple_configs(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        items = [
            {"foo": "FOO", "bar": "BAR"},
            {"foo": "FOO1", "bar": "BAR1"},
        ]

        formats = {
            "json": [
                b'[\n{"bar": "BAR"}\n]',
                b'[\n{"bar": "BAR1"}\n]',
            ],
            "xml": [
                (
                    b'<?xml version="1.0" encoding="latin-1"?>\n'
                    b"<items>\n  <item>\n    <foo>FOO</foo>\n  </item>\n</items>"
                ),
                (
                    b'<?xml version="1.0" encoding="latin-1"?>\n'
                    b"<items>\n  <item>\n    <foo>FOO1</foo>\n  </item>\n</items>"
                ),
            ],
            "csv": [
                b"foo,bar\r\nFOO,BAR\r\n",
                b"foo,bar\r\nFOO1,BAR1\r\n",
            ],
        }

        settings = {
            "FEEDS": {
                unique_path(tmp_path) / self._file_mark: {
                    "format": "json",
                    "indent": 0,
                    "fields": ["bar"],
                    "encoding": "utf-8",
                },
                unique_path(tmp_path) / self._file_mark: {
                    "format": "xml",
                    "indent": 2,
                    "fields": ["foo"],
                    "encoding": "latin-1",
                },
                unique_path(tmp_path) / self._file_mark: {
                    "format": "csv",
                    "indent": None,
                    "fields": ["foo", "bar"],
                    "encoding": "utf-8",
                },
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        }
        data = await export_batches(mockserver, items, settings)
        for fmt, expected in formats.items():
            for expected_batch, got_batch in zip(expected, data[fmt], strict=True):
                assert got_batch == expected_batch

    @coroutine_test
    async def test_batch_item_count_feeds_setting(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        items = [{"foo": "FOO"}, {"foo": "FOO1"}]
        formats = {
            "json": [
                b'[{"foo": "FOO"}]',
                b'[{"foo": "FOO1"}]',
            ],
        }
        settings = {
            "FEEDS": {
                unique_path(tmp_path) / self._file_mark: {
                    "format": "json",
                    "indent": None,
                    "encoding": "utf-8",
                    "batch_item_count": 1,
                },
            },
        }
        data = await export_batches(mockserver, items, settings)
        for fmt, expected in formats.items():
            for expected_batch, got_batch in zip(expected, data[fmt], strict=True):
                assert got_batch == expected_batch

    @coroutine_test
    async def test_batch_item_count_with_item_processor(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        def split_foo(item):
            for value in item["foo"].split(","):
                yield {"foo": value}

        items = [{"foo": "FOO,FOO1"}, {"foo": "FOO2"}]
        formats = {
            "json": [
                b'[{"foo": "FOO"}]',
                b'[{"foo": "FOO1"}]',
                b'[{"foo": "FOO2"}]',
            ],
        }
        settings = {
            "FEEDS": {
                unique_path(tmp_path) / self._file_mark: {
                    "format": "json",
                    "indent": None,
                    "encoding": "utf-8",
                    "batch_item_count": 1,
                    "item_processor": split_foo,
                },
            },
        }
        data = await export_batches(mockserver, items, settings)
        for fmt, expected in formats.items():
            for expected_batch, got_batch in zip(expected, data[fmt], strict=True):
                assert got_batch == expected_batch

    @coroutine_test
    async def test_batch_path_differ(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """
        Test that the name of all batch files differ from each other.
        So %(batch_id)d replaced with the current id.
        """
        items = [
            MyItem({"foo": "bar1", "egg": "spam1"}),
            MyItem({"foo": "bar2", "egg": "spam2", "baz": "quux2"}),
            MyItem({"foo": "bar3", "baz": "quux3"}),
        ]
        settings = {
            "FEEDS": {
                unique_path(tmp_path) / "%(batch_id)d": {
                    "format": "json",
                },
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        }
        data = await export_batches(mockserver, items, settings)
        assert len(items) == len(data["json"])

    @coroutine_test
    async def test_jobdir_resume(self, mockserver: MockServer, tmp_path: Path) -> None:
        """Resuming a crawl with the same JOBDIR continues the batch numbering
        instead of overwriting the batches of the previous run (#5153)."""
        settings = {
            "FEEDS": {
                unique_path(tmp_path) / "%(batch_id)d.jsonl": {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "JOBDIR": str(tmp_path / "job"),
        }
        run1 = await export_batches(
            mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings
        )
        run2 = await export_batches(
            mockserver, [{"foo": "bar3"}, {"foo": "bar4"}], settings
        )
        assert run2["jsonlines"] == [
            *run1["jsonlines"],
            b'{"foo": "bar3"}\n',
            b'{"foo": "bar4"}\n',
        ]

    @coroutine_test
    async def test_jobdir_resume_new_feed(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A feed added when resuming a crawl starts at batch 1."""
        dir_a = unique_path(tmp_path)
        dir_b = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {build_url(dir_a / "%(batch_id)d.jsonl"): {"format": "jsonlines"}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "JOBDIR": str(tmp_path / "job"),
        }
        items = [{"foo": "bar1"}, {"foo": "bar2"}]
        await crawl_items(mockserver, items, settings)
        settings["FEEDS"][build_url(dir_b / "%(batch_id)d.jsonl")] = {
            "format": "jsonlines"
        }
        await crawl_items(mockserver, items, settings)
        assert sorted(p.name for p in dir_a.iterdir()) == [
            "1.jsonl",
            "2.jsonl",
            "3.jsonl",
            "4.jsonl",
        ]
        assert sorted(p.name for p in dir_b.iterdir()) == ["1.jsonl", "2.jsonl"]

    @coroutine_test
    async def test_jobdir_resume_remote_partial_batch(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """The target of a batch in a remote storage cannot be removed, so a
        batch that an interrupted run left half-written keeps its number and is
        not written again on resume."""
        uri_template = "memory://feeds/%(batch_id)d.jsonl"
        MemoryFeedStorage.targets.clear()
        settings: dict[str, Any] = {
            "FEEDS": {uri_template: {"format": "jsonlines"}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "JOBDIR": str(tmp_path / "job"),
            "FEED_STORAGES": {"memory": MemoryFeedStorage},
        }
        await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        # Simulate a crash while batch 2 was being written: the remote storage
        # has no partial target to remove, and the batch is not complete.
        manifest = tmp_path / "job" / "feed_manifest.json"
        with manifest.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "feed": uri_template,
                        "batch_id": 2,
                        "uri": "memory://feeds/2.jsonl",
                        "first_item": 2,
                    }
                )
                + "\n"
            )
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        # Batch 2 kept its number but was not stored, so the new entries went
        # to batch 3 instead.
        assert "memory://feeds/3.jsonl" in MemoryFeedStorage.targets
        assert MemoryFeedStorage.targets["memory://feeds/3.jsonl"] == (
            b'{"foo": "bar2"}\n'
        )

    @coroutine_test
    async def test_jobdir_resume_ignores_partial_batch(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A batch that an interrupted run left half-written is dropped, and
        the resumed run starts a new batch after the last complete one, instead
        of appending to the leftovers."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        # Simulate a crash while batch 2 was being written: its file has one
        # entry, and the manifest records the batch as started, not complete.
        (dir_path / "2.jsonl").write_bytes(b'{"foo": "bar3"}\n')
        manifest = tmp_path / "job" / "feed_manifest.json"
        with manifest.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "feed": build_url(dir_path / "%(batch_id)d.jsonl"),
                        "batch_id": 2,
                        "uri": build_url(dir_path / "2.jsonl"),
                        "first_item": 3,
                    }
                )
                + "\n"
            )
        await crawl_items(mockserver, [{"foo": "bar4"}, {"foo": "bar5"}], settings)
        assert (dir_path / "1.jsonl").read_bytes() == (
            b'{"foo": "bar1"}\n{"foo": "bar2"}\n'
        )
        assert (dir_path / "2.jsonl").read_bytes() == (
            b'{"foo": "bar4"}\n{"foo": "bar5"}\n'
        )
        assert not (dir_path / "3.jsonl").exists()

    @coroutine_test
    async def test_jobdir_resume_reads_journal_events(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A manifest left by a run that was killed before it could compact it
        is read back from its separate event lines: a batch start followed by
        its completion, and a discarded batch."""
        dir_path = unique_path(tmp_path)
        uri_template = build_url(dir_path / "%(batch_id)d.jsonl")
        settings: dict[str, Any] = {
            "FEEDS": {uri_template: {"format": "jsonlines"}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        manifest = tmp_path / "job" / "feed_manifest.json"
        manifest.write_text(
            "\n".join(
                json.dumps(event)
                for event in (
                    {
                        "feed": uri_template,
                        "batch_id": 1,
                        "uri": build_url(dir_path / "1.jsonl"),
                        "first_item": 1,
                    },
                    {"feed": uri_template, "batch_id": 1, "item_count": 1},
                    {
                        "feed": uri_template,
                        "batch_id": 2,
                        "uri": build_url(dir_path / "2.jsonl"),
                        "first_item": 2,
                    },
                    {"feed": uri_template, "batch_id": 2, "discard": True},
                )
            )
            + "\n",
            encoding="utf-8",
        )
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        # Batch 1 is complete and batch 2 was discarded, so its number is
        # reused by the new entries.
        assert (dir_path / "1.jsonl").read_bytes() == b'{"foo": "bar1"}\n'
        assert (dir_path / "2.jsonl").read_bytes() == b'{"foo": "bar2"}\n'

    @coroutine_test
    async def test_jobdir_resume_after_compact(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Each resumed run compacts the manifest into one line per batch, and
        the run that follows still reads the batches back from those lines."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        manifest = tmp_path / "job" / "feed_manifest.json"
        assert len(manifest.read_text(encoding="utf-8").splitlines()) == 1
        await crawl_items(mockserver, [{"foo": "bar3"}, {"foo": "bar4"}], settings)
        await crawl_items(mockserver, [{"foo": "bar5"}], settings)
        assert (dir_path / "1.jsonl").read_bytes() == (
            b'{"foo": "bar1"}\n{"foo": "bar2"}\n'
        )
        assert (dir_path / "2.jsonl").read_bytes() == (
            b'{"foo": "bar3"}\n{"foo": "bar4"}\n'
        )
        assert (dir_path / "3.jsonl").read_bytes() == b'{"foo": "bar5"}\n'

    @coroutine_test
    async def test_jobdir_resume_keeps_stored_batches(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """Resuming a crawl never rewrites the batches that the previous run
        already stored, so their entries are not exported twice."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(
            mockserver, [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}], settings
        )
        await crawl_items(mockserver, [{"foo": "bar4"}], settings)
        assert (dir_path / "1.jsonl").read_bytes() == (
            b'{"foo": "bar1"}\n{"foo": "bar2"}\n'
        )
        assert (dir_path / "2.jsonl").read_bytes() == b'{"foo": "bar3"}\n'
        assert (dir_path / "3.jsonl").read_bytes() == b'{"foo": "bar4"}\n'

    @coroutine_test
    async def test_jobdir_resume_manifest(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """The feed manifest records the URI, the entry range and the entry
        count of every stored batch, so that they can be looked up."""
        dir_path = unique_path(tmp_path)
        uri_template = build_url(dir_path / "%(batch_id)d.jsonl")
        settings: dict[str, Any] = {
            "FEEDS": {uri_template: {"format": "jsonlines"}},
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(
            mockserver, [{"foo": "bar1"}, {"foo": "bar2"}, {"foo": "bar3"}], settings
        )
        manifest = tmp_path / "job" / "feed_manifest.json"
        records = [
            json.loads(line)
            for line in manifest.read_text(encoding="utf-8").splitlines()
        ]
        assert records == [
            {
                "feed": uri_template,
                "batch_id": 1,
                "uri": build_url(dir_path / "1.jsonl"),
                "first_item": 1,
                "last_item": 2,
                "item_count": 2,
                "complete": True,
                "pre_existing": False,
            },
            {
                "feed": uri_template,
                "batch_id": 2,
                "uri": build_url(dir_path / "2.jsonl"),
                "first_item": 3,
                "last_item": 3,
                "item_count": 1,
                "complete": True,
                "pre_existing": False,
            },
        ]

    @coroutine_test
    async def test_jobdir_resume_ignores_corrupt_manifest_line(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A run killed while appending to the manifest leaves a partial last
        line; the next run ignores it and resumes from the complete records
        before it."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        manifest = tmp_path / "job" / "feed_manifest.json"
        with manifest.open("a", encoding="utf-8") as f:
            f.write('{"feed": "file:///partial", "batch_id":')
        await crawl_items(mockserver, [{"foo": "bar3"}], settings)
        assert (dir_path / "2.jsonl").read_bytes() == b'{"foo": "bar3"}\n'
        # The complete record of batch 1 survived the corrupted line.
        records = [
            json.loads(line)
            for line in manifest.read_text(encoding="utf-8").splitlines()
            if line.endswith("}")
        ]
        assert [r["batch_id"] for r in records if "discard" not in r] == [1, 2]

    @coroutine_test
    async def test_jobdir_resume_ignores_garbage_manifest(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A manifest with lines that are not feed records is ignored, and the
        resume proceeds as if it were empty instead of failing the crawl."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        manifest = tmp_path / "job" / "feed_manifest.json"
        with manifest.open("w", encoding="utf-8") as f:
            for line in ("[]", "{}", '{"feed": 1, "batch_id": "x"}', "not json"):
                f.write(line + "\n")
        await crawl_items(mockserver, [{"foo": "bar2"}], settings)
        # The unusable lines are ignored, and resume falls back to the batch
        # ID in spider.state, so the new entries do not overwrite batch 1.
        assert (dir_path / "1.jsonl").read_bytes() == b'{"foo": "bar1"}\n'
        assert (dir_path / "2.jsonl").read_bytes() == b'{"foo": "bar2"}\n'

    @coroutine_test
    async def test_jobdir_resume_keeps_preexisting_target(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A batch whose target already existed before an interrupted run
        started writing it is not removed on resume, even though the run left
        it half-written: it is not the run's to delete."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 2,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        # Simulate a crash during batch 2 after appending to a target that
        # already existed (e.g. a resumed run over a stale file).
        (dir_path / "2.jsonl").write_bytes(b"previous\n")
        manifest = tmp_path / "job" / "feed_manifest.json"
        with manifest.open("a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "feed": build_url(dir_path / "%(batch_id)d.jsonl"),
                        "batch_id": 2,
                        "uri": build_url(dir_path / "2.jsonl"),
                        "first_item": 3,
                        "pre_existing": True,
                    }
                )
                + "\n"
            )
        await crawl_items(mockserver, [{"foo": "bar3"}, {"foo": "bar4"}], settings)
        # The pre-existing target was left untouched, and the new entries
        # went to a new batch instead of into it.
        assert (dir_path / "2.jsonl").read_bytes() == b"previous\n"
        assert (dir_path / "3.jsonl").read_bytes() == (
            b'{"foo": "bar3"}\n{"foo": "bar4"}\n'
        )

    @coroutine_test
    async def test_jobdir_resume_without_manifest(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        """A job directory written by a Scrapy version that did not keep a feed
        manifest resumes from the batch ID in the spider state instead."""
        dir_path = unique_path(tmp_path)
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(dir_path / "%(batch_id)d.jsonl"): {"format": "jsonlines"}
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "JOBDIR": str(tmp_path / "job"),
        }
        await crawl_items(mockserver, [{"foo": "bar1"}], settings)
        (tmp_path / "job" / "feed_manifest.json").unlink()

        batch_ids: list[int] = []

        def on_initialized(slots: list[FeedSlot]) -> None:
            batch_ids.extend(slot.batch_id for slot in slots)

        crawler = get_crawler(settings_dict=settings)
        crawler.signals.connect(on_initialized, signals.feed_slots_initialized)
        await crawler.crawl_async()
        assert batch_ids == [2]

    @pytest.mark.parametrize(
        ("jobdir", "batch_id"), [(False, 1), (True, 3)], ids=["no-jobdir", "resume"]
    )
    @coroutine_test
    async def test_feed_slots_initialized(
        self, mockserver: MockServer, tmp_path: Path, jobdir: bool, batch_id: int
    ) -> None:
        settings: dict[str, Any] = {
            "FEEDS": {
                build_url(unique_path(tmp_path) / "%(batch_id)d.jsonl"): {
                    "format": "jsonlines"
                }
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        }
        if jobdir:
            settings["JOBDIR"] = str(tmp_path / "job")
            await crawl_items(mockserver, [{"foo": "bar1"}, {"foo": "bar2"}], settings)
        batch_ids: list[int] = []

        def on_initialized(slots: list[FeedSlot]) -> None:
            batch_ids.extend(slot.batch_id for slot in slots)

        crawler = get_crawler(settings_dict=settings)
        crawler.signals.connect(on_initialized, signals.feed_slots_initialized)
        await crawler.crawl_async()
        assert batch_ids == [batch_id]

    @coroutine_test
    async def test_stats_batch_file_success(
        self, mockserver: MockServer, tmp_path: Path
    ) -> None:
        settings = {
            "FEEDS": {
                build_url(str(unique_path(tmp_path) / self._file_mark)): {
                    "format": "json",
                }
            },
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
        }
        crawler = get_crawler(ItemSpider, settings)
        await crawler.crawl_async(total=2, mockserver=mockserver)
        assert "feedexport/success_count/FileFeedStorage" in crawler.stats.get_stats()
        assert crawler.stats.get_value("feedexport/success_count/FileFeedStorage") == 12

    @pytest.mark.requires_boto3
    @coroutine_test
    async def test_s3_export(
        self, mockserver: MockServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Force the blocking boto3 code path so that the botocore Stubber below
        # intercepts the uploads.
        monkeypatch.setattr(
            "scrapy.extensions.feedexport.is_aioboto3_available", lambda: False
        )
        bucket = "mybucket"
        items = [
            MyItem({"foo": "bar1", "egg": "spam1"}),
            MyItem({"foo": "bar2", "egg": "spam2", "baz": "quux2"}),
            MyItem({"foo": "bar3", "baz": "quux3"}),
        ]

        class CustomS3FeedStorage(S3FeedStorage):
            from botocore.stub import Stubber  # noqa: PLC0415

            stubs: list[Stubber] = []

            def open(self, *args, **kwargs):
                from botocore import __version__ as botocore_version  # noqa: PLC0415
                from botocore.stub import ANY, Stubber  # noqa: PLC0415

                expected_params = {
                    "Body": ANY,
                    "Bucket": bucket,
                    "Key": ANY,
                }
                if Version(botocore_version) >= Version("1.36.0"):
                    expected_params["ChecksumAlgorithm"] = ANY

                stub = Stubber(self.s3_client)
                stub.activate()
                CustomS3FeedStorage.stubs.append(stub)
                stub.add_response(
                    "put_object",
                    expected_params=expected_params,
                    service_response={},
                )
                return super().open(*args, **kwargs)

        key = "export.csv"
        uri = f"s3://{bucket}/{key}/%(batch_id)d.json"
        batch_item_count = 1
        settings = {
            "AWS_ACCESS_KEY_ID": "access_key",
            "AWS_SECRET_ACCESS_KEY": "secret_key",
            "FEED_EXPORT_BATCH_ITEM_COUNT": batch_item_count,
            "FEED_STORAGES": {
                "s3": CustomS3FeedStorage,
            },
            "FEEDS": {
                uri: {
                    "format": "json",
                },
            },
        }

        crawler = await crawl_items(mockserver, items, settings)

        assert len(CustomS3FeedStorage.stubs) == len(items)
        for stub in CustomS3FeedStorage.stubs:
            stub.assert_no_pending_responses()
        assert (
            "feedexport/success_count/CustomS3FeedStorage" in crawler.stats.get_stats()
        )
        assert (
            crawler.stats.get_value("feedexport/success_count/CustomS3FeedStorage") == 3
        )

    @pytest.mark.requires_aioboto3
    @pytest.mark.only_asyncio
    @coroutine_test
    async def test_s3_export_async(
        self, mockserver: MockServer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One batch per item is uploaded through the aioboto3 code path."""
        import aioboto3  # noqa: PLC0415

        bucket = "mybucket"
        items = [
            MyItem({"foo": "bar1", "egg": "spam1"}),
            MyItem({"foo": "bar2", "egg": "spam2", "baz": "quux2"}),
            MyItem({"foo": "bar3", "baz": "quux3"}),
        ]

        upload_fileobj = mock.AsyncMock()

        def make_client(self, *args, **kwargs):
            client = mock.MagicMock()
            client.upload_fileobj = upload_fileobj
            client_cm = mock.MagicMock()
            client_cm.__aenter__ = mock.AsyncMock(return_value=client)
            client_cm.__aexit__ = mock.AsyncMock(return_value=False)
            return client_cm

        monkeypatch.setattr(aioboto3.Session, "client", make_client)

        key = "export.csv"
        uri = f"s3://{bucket}/{key}/%(batch_id)d.json"
        settings = {
            "AWS_ACCESS_KEY_ID": "access_key",
            "AWS_SECRET_ACCESS_KEY": "secret_key",
            "FEED_EXPORT_BATCH_ITEM_COUNT": 1,
            "FEEDS": {uri: {"format": "json"}},
        }

        crawler = await crawl_items(mockserver, items, settings)

        assert upload_fileobj.await_count == len(items)
        for call in upload_fileobj.await_args_list:
            assert call.kwargs["Bucket"] == bucket
        assert crawler.stats
        assert crawler.stats.get_value("feedexport/success_count/S3FeedStorage") == 3


class TestFeedManifestErrors:
    """The feed manifest is auxiliary: failures to read or write it are logged
    and the crawl goes on without it."""

    def test_unreadable(self, tmp_path: Path) -> None:
        path = tmp_path / "feed_manifest.json"
        path.mkdir()
        assert _FeedManifest(path).shards("file:///x") == []

    def test_unwritable(self, tmp_path: Path) -> None:
        path = tmp_path / "feed_manifest.json"
        path.mkdir()
        manifest = _FeedManifest(path)
        # A write failure is logged, not raised, and recording continues to be
        # possible once the file is writable again.
        manifest.begin_shard("file:///x", 1, "file:///x/1.jsonl", 1)
        manifest.complete_shard("file:///x", 1, 2)
        assert manifest.next_batch_id("file:///x") == 2

    def test_non_local_uri(self) -> None:
        assert _local_path("s3://bucket/1.jsonl") is None
        assert _local_path("file:///tmp/1.jsonl") == Path("/tmp/1.jsonl")
        assert _local_path("/tmp/1.jsonl") == Path("/tmp/1.jsonl")

    def test_unremovable_partial_target(self, tmp_path: Path) -> None:
        """A partial file that cannot be removed keeps the sequence number of
        its batch, so that the resumed run does not write over it."""
        shard = {"uri": "file:///tmp/partial.jsonl", "pre_existing": False}
        with mock.patch.object(Path, "unlink", side_effect=OSError("denied")):
            assert FeedExporter._remove_partial_target(shard, mock.MagicMock()) is False

    def test_missing_partial_target(self, tmp_path: Path) -> None:
        """A partial file that is already gone is treated as removed."""
        shard = {
            "uri": f"file://{tmp_path}/missing.jsonl",
            "pre_existing": False,
        }
        assert FeedExporter._remove_partial_target(shard, mock.MagicMock()) is True

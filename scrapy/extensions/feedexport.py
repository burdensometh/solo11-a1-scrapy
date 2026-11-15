"""
Feed Exports extension

See documentation in docs/topics/feed-exports.rst
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import sys
import warnings
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path, PureWindowsPath
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Protocol, Self, TypeAlias, cast
from urllib.parse import unquote, urlparse
from uuid import uuid4

from twisted.internet.defer import Deferred, DeferredList, DeferredLock
from w3lib.url import file_uri_to_path
from zope.interface import Interface

from scrapy import Spider, signals
from scrapy.exceptions import NotConfigured, ScrapyDeprecationWarning
from scrapy.extensions.postprocessing import PostProcessingManager
from scrapy.utils._ftp import ftp_store_file
from scrapy.utils.asyncio import is_asyncio_available, run_in_thread
from scrapy.utils.boto import _get_max_pool_connections, is_aioboto3_available
from scrapy.utils.conf import _job_dir, feed_complete_default_values_from_settings
from scrapy.utils.defer import (
    _maybe_coroutine,
    deferred_from_coro,
    ensure_awaitable,
    maybe_deferred_to_future,
)
from scrapy.utils.misc import build_from_crawler, load_object
from scrapy.utils.python import without_none_values

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from _typeshed import OpenBinaryMode

    from scrapy.crawler import Crawler
    from scrapy.exporters import BaseItemExporter
    from scrapy.settings import BaseSettings, Settings


logger = logging.getLogger(__name__)


# Printf-style placeholders (e.g. %(time)s) used to build feed URIs. Any other
# percent character in a URI (e.g. percent-encoding such as %20 or %23) must be
# treated as a literal rather than as the start of a placeholder.
_FEED_URI_PLACEHOLDER_RE = re.compile(
    r"%\([^)]+\)[-+ #0]*(?:\d+|\*)?(?:\.(?:\d+|\*))?[diouxXeEfFgGcrsa]"
)


def apply_uri_params(uri_template: str, uri_params: dict[str, Any]) -> str:
    """Return *uri_template* with its ``%(...)s`` placeholders replaced using
    *uri_params*, leaving any other percent character untouched.

    This allows feed URIs to contain percent-encoded characters (e.g. ``%20``
    in a path with spaces or ``%23`` in FTP credentials) without them being
    misinterpreted as printf-style formatting directives.
    """
    parts: list[str] = []
    last = 0
    for match in _FEED_URI_PLACEHOLDER_RE.finditer(uri_template):
        parts.append(uri_template[last : match.start()].replace("%", "%%"))
        parts.append(match.group(0))
        last = match.end()
    parts.append(uri_template[last:].replace("%", "%%"))
    return "".join(parts) % uri_params


UriParamsCallableT: TypeAlias = Callable[
    [dict[str, Any], Spider], dict[str, Any] | None
]

_ItemProcessor: TypeAlias = Callable[[Any], Iterable[Any]]


_FEED_MODES: frozenset[str] = frozenset({"append", "create", "overwrite"})

_MANIFEST_FILENAME = "feed_manifest.json"


def _get_mode(storage: Any, feed_options: dict[str, Any] | None, legacy: str) -> str:
    """Return the mode of *storage*, based on the *mode* feed option, or on the
    deprecated *overwrite* feed option, or, if neither is set, on *legacy*,
    which must be the mode that the storage used before the *mode* feed option
    existed."""
    feed_options = feed_options or {}
    mode = feed_options.get("mode")
    if mode is None:
        overwrite = feed_options.get("overwrite")
        mode = legacy if overwrite is None else "overwrite" if overwrite else "append"
    _check_mode(mode, storage)
    return mode


def _check_mode(mode: str, storage: Any, uri: str | None = None) -> None:
    if mode not in _FEED_MODES:
        raise ValueError(
            f"Invalid feed mode: {mode!r}. Supported modes: "
            f"{', '.join(sorted(_FEED_MODES))}."
        )
    supported: frozenset[str] | None = getattr(storage, "supported_modes", None)
    if supported is not None and mode not in supported:
        suffix = f" (feed URI: {uri})" if uri else ""
        raise ValueError(
            f"{type(storage).__name__} does not support the {mode!r} feed "
            f"mode{suffix}. Supported modes: {', '.join(sorted(supported))}."
        )


def _local_path(uri: str) -> Path | None:
    """Return the local filesystem path of *uri*, or ``None`` if *uri* points
    to another storage backend."""
    if uri.startswith("file:"):
        return Path(file_uri_to_path(uri))
    if urlparse(uri).scheme and not PureWindowsPath(uri).drive:
        return None
    return Path(uri)


class _FeedManifest:
    """Record the batches of each feed of a job, their target URI, the range
    of exported entries that they cover, and whether they were completely
    stored.

    Events are appended to a JSON Lines file as they happen, so an interrupted
    run loses at most the last, partial line, and the next run can tell the
    complete batches from the interrupted ones. On a clean close the file is
    rewritten with one final line per batch, which makes it directly readable.
    """

    def __init__(self, path: Path):
        self.path: Path = path
        self._file: IO[str] | None = None
        self._feeds: dict[str, dict[int, dict[str, Any]]] = self._load()

    def _load(self) -> dict[str, dict[int, dict[str, Any]]]:
        feeds: dict[str, dict[int, dict[str, Any]]] = {}
        try:
            lines = self.path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return feeds
        except OSError as e:
            logger.error(f"Could not read the feed manifest {self.path}: {e}")
            return feeds
        for line in lines:
            try:
                event = json.loads(line)
            except ValueError:
                # A run killed while appending leaves a partial last line.
                continue
            if not isinstance(event, dict):
                continue
            feed, batch_id = event.get("feed"), event.get("batch_id")
            if not isinstance(feed, str) or not isinstance(batch_id, int):
                continue
            shards = feeds.setdefault(feed, {})
            if isinstance(event.get("uri"), str) and isinstance(
                event.get("first_item"), int
            ):
                first_item = event["first_item"]
                item_count = event.get("item_count")
                shards[batch_id] = {
                    "batch_id": batch_id,
                    "uri": event["uri"],
                    "first_item": first_item,
                    "last_item": first_item - 1,
                    "item_count": 0,
                    "complete": False,
                    "pre_existing": bool(event.get("pre_existing")),
                }
                if isinstance(item_count, int) and event.get("complete"):
                    shards[batch_id]["item_count"] = item_count
                    shards[batch_id]["last_item"] = first_item + item_count - 1
                    shards[batch_id]["complete"] = True
            elif event.get("discard"):
                shards.pop(batch_id, None)
            elif isinstance(event.get("item_count"), int) and batch_id in shards:
                shard = shards[batch_id]
                shard["item_count"] = event["item_count"]
                shard["last_item"] = shard["first_item"] + event["item_count"] - 1
                shard["complete"] = True
        return feeds

    def shards(self, uri_template: str) -> list[dict[str, Any]]:
        return sorted(
            self._feeds.get(uri_template, {}).values(), key=lambda s: s["batch_id"]
        )

    def complete_shards(self, uri_template: str) -> list[dict[str, Any]]:
        return [s for s in self.shards(uri_template) if s["complete"]]

    def next_batch_id(self, uri_template: str) -> int:
        """Return the sequence number of the batch that a resumed crawl must
        start with: the one after the last recorded batch.

        Batches that are recorded but were not completely stored still count
        here, because their target may be left behind and must not be written
        again."""
        return max((s["batch_id"] for s in self.shards(uri_template)), default=0) + 1

    def next_item_index(self, uri_template: str) -> int:
        """Return the 1-based index of the first exported entry that a resumed
        crawl must write: the one after the entries of the completely stored
        batches."""
        return (
            max((s["last_item"] for s in self.complete_shards(uri_template)), default=0)
            + 1
        )

    def begin_shard(
        self,
        uri_template: str,
        batch_id: int,
        uri: str,
        first_item: int,
        *,
        pre_existing: bool = False,
    ) -> None:
        """Record that the batch *batch_id* of *uri_template* is being written
        to *uri*, starting with the exported entry *first_item*.

        *pre_existing* tells whether *uri* already existed, so that a later run
        never removes a target that this run did not create."""
        self._feeds.setdefault(uri_template, {}).pop(batch_id, None)
        self._feeds[uri_template][batch_id] = {
            "batch_id": batch_id,
            "uri": uri,
            "first_item": first_item,
            "last_item": first_item - 1,
            "item_count": 0,
            "complete": False,
            "pre_existing": pre_existing,
        }
        self._append(
            {
                "feed": uri_template,
                "batch_id": batch_id,
                "uri": uri,
                "first_item": first_item,
                "pre_existing": pre_existing,
            }
        )

    def complete_shard(self, uri_template: str, batch_id: int, item_count: int) -> None:
        """Record that the batch *batch_id* of *uri_template* was completely
        stored, with *item_count* exported entries."""
        shard = self._feeds[uri_template][batch_id]
        shard["item_count"] = item_count
        shard["last_item"] = shard["first_item"] + item_count - 1
        shard["complete"] = True
        self._append(
            {"feed": uri_template, "batch_id": batch_id, "item_count": item_count}
        )

    def incomplete_shards(self, uri_template: str) -> list[dict[str, Any]]:
        """Return the batches of *uri_template* that were not completely
        stored, e.g. because the run that was writing them was interrupted."""
        return [s for s in self.shards(uri_template) if not s["complete"]]

    def forget_shard(self, uri_template: str, batch_id: int) -> None:
        """Forget the batch *batch_id* of *uri_template*, because its target
        was removed and its sequence number can be used again."""
        shards = self._feeds.get(uri_template)
        if shards is not None and shards.pop(batch_id, None) is not None:
            self._append({"feed": uri_template, "batch_id": batch_id, "discard": True})

    def compact(self) -> None:
        """Replace the journal with one final line per recorded batch."""
        if self._file is None:
            return
        self._file.close()
        self._file = None
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            for uri_template in sorted(self._feeds):
                for shard in self.shards(uri_template):
                    f.write(json.dumps({"feed": uri_template, **shard}) + "\n")
        tmp_path.replace(self.path)

    def _append(self, event: dict[str, Any]) -> None:
        try:
            if self._file is None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self._file = self.path.open("a", encoding="utf-8")
            self._file.write(json.dumps(event) + "\n")
            self._file.flush()
        except OSError as e:
            # The manifest is auxiliary: a crawl must go on without it.
            logger.error(f"Could not update the feed manifest {self.path}: {e}")
            self._file = None


class ItemFilter:
    """
    This will be used by FeedExporter to decide if an item should be allowed
    to be exported to a particular feed.

    :param feed_options: feed specific options passed from FeedExporter
    :type feed_options: dict
    """

    feed_options: dict[str, Any] | None
    item_classes: tuple[type, ...]

    def __init__(self, feed_options: dict[str, Any] | None) -> None:
        self.feed_options = feed_options
        if feed_options is not None:
            self.item_classes = tuple(
                load_object(item_class)
                for item_class in feed_options.get("item_classes") or ()
            )
        else:
            self.item_classes = ()

    def accepts(self, item: Any) -> bool:
        """
        Return ``True`` if `item` should be exported or ``False`` otherwise.

        :param item: scraped item which user wants to check if is acceptable
        :type item: :ref:`Scrapy items <topics-items>`
        :return: `True` if accepted, `False` otherwise
        :rtype: bool
        """
        if self.item_classes:
            return isinstance(item, self.item_classes)
        return True  # accept all items by default


class _IFeedStorage(Interface):  # type: ignore[misc]  # pragma: no cover
    # pylint: disable=no-self-argument

    def __init__(uri, *, feed_options=None): ...  # type: ignore[no-untyped-def]  # pylint: disable=super-init-not-called

    def open(spider): ...  # type: ignore[no-untyped-def]

    def store(file): ...  # type: ignore[no-untyped-def]


class FeedStorageProtocol(Protocol):
    """Protocol that all Feed Storages must follow.

    :param uri: Feed URI.

    :param feed_options: :ref:`Feed options <feed-options>` of the feed.

    A feed storage may define a ``from_crawler(cls, crawler, uri, *,
    feed_options=None)`` class method instead of ``__init__``, to also get the
    :class:`~scrapy.crawler.Crawler` object.
    """

    def open(self, spider: Spider) -> IO[bytes] | Coroutine[Any, Any, IO[bytes]]:
        """Open the storage for *spider* and return the file-like object that
        item exporters write into.

        .. versionchanged:: 2.20.0
           This method may now be a coroutine function (``async def``).
        """

    def store(
        self, file: IO[bytes]
    ) -> Coroutine[Any, Any, None] | Deferred[None] | None:
        """Store *file*, the file-like object returned by :meth:`open`, and
        close it.

        .. versionchanged:: 2.20.0
           This method may now be a coroutine function (``async def``).
        """


class BlockingFeedStorage(ABC):
    """Base class for feed storages that store feeds using blocking code."""

    def open(self, spider: Spider) -> IO[bytes]:
        path = spider.crawler.settings["FEED_TEMPDIR"]
        if path and not Path(path).is_dir():
            raise OSError("Not a Directory: " + str(path))

        return NamedTemporaryFile(prefix="feed-", dir=path)

    def store(self, file: IO[bytes]) -> Deferred[None]:
        return deferred_from_coro(run_in_thread(self._store_in_thread, file))

    @abstractmethod
    def _store_in_thread(self, file: IO[bytes]) -> None:
        """Store *file* and close it.

        This method runs in a separate thread, so it may use blocking code.
        """
        raise NotImplementedError


class StdoutFeedStorage:
    """:ref:`Standard output <topics-feed-storage-stdout>` storage backend."""

    # The mode is irrelevant here: writing to a stream cannot destroy data.
    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        _stdout: IO[bytes] | None = None,
        *,
        feed_options: dict[str, Any] | None = None,
    ):
        if not _stdout:
            _stdout = sys.stdout.buffer
        self._stdout: IO[bytes] = _stdout

    def open(self, spider: Spider) -> IO[bytes]:
        return self._stdout

    def store(self, file: IO[bytes]) -> Deferred[None] | None:
        pass


_WRITE_MODES: dict[str, OpenBinaryMode] = {
    "append": "ab",
    "create": "xb",
    "overwrite": "wb",
}


class FileFeedStorage:
    """:ref:`Local filesystem <topics-feed-storage-fs>` storage backend.

    *uri* may be a ``file://`` URI or a plain path. Missing parent directories
    are created when the feed is opened.
    """

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(self, uri: str, *, feed_options: dict[str, Any] | None = None):
        self.path: str = file_uri_to_path(uri) if uri.startswith("file:") else uri
        self.write_mode: OpenBinaryMode = _WRITE_MODES[
            _get_mode(self, feed_options, "append")
        ]

    def open(self, spider: Spider) -> IO[bytes]:
        dirname = Path(self.path).parent
        if dirname and not dirname.exists():
            dirname.mkdir(parents=True)
        return Path(self.path).open(self.write_mode)

    def store(self, file: IO[bytes]) -> Deferred[None] | None:
        file.close()
        return None


class S3FeedStorage(BlockingFeedStorage):
    """:ref:`Amazon S3 <topics-feed-storage-s3>` storage backend."""

    supported_modes: frozenset[str] = frozenset({"create", "overwrite"})

    def __init__(
        self,
        uri: str,
        access_key: str | None = None,
        secret_key: str | None = None,
        acl: str | None = None,
        endpoint_url: str | None = None,
        *,
        feed_options: dict[str, Any] | None = None,
        session_token: str | None = None,
        region_name: str | None = None,
        max_pool_connections: int | None = None,
        upload_timeout: float | None = None,
    ):
        try:
            import boto3.session  # noqa: PLC0415
        except ImportError:
            raise NotConfigured("missing boto3 library") from None
        from botocore.config import Config  # noqa: PLC0415

        u = urlparse(uri)
        assert u.hostname
        self.bucketname: str = u.hostname
        self.access_key: str | None = u.username or access_key
        self.secret_key: str | None = u.password or secret_key
        self.session_token: str | None = session_token
        self.keyname: str = u.path[1:]  # remove first "/"
        self.acl: str | None = acl
        self.endpoint_url: str | None = endpoint_url
        self.region_name: str | None = region_name
        self.max_pool_connections: int | None = max_pool_connections
        self.upload_timeout: float | None = upload_timeout

        config_kwargs: dict[str, Any] = {}
        if max_pool_connections is not None:
            config_kwargs["max_pool_connections"] = max_pool_connections
        if upload_timeout is not None:
            config_kwargs["read_timeout"] = upload_timeout

        self._client_kwargs: dict[str, Any] = {
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
            "aws_session_token": self.session_token,
            "endpoint_url": self.endpoint_url,
            "region_name": self.region_name,
            "config": Config(**config_kwargs),
        }

        # Synchronous boto3 client, used when asyncio support or aioboto3 is not
        # available (its calls are then run in a thread).
        boto3_session = boto3.session.Session()
        self.s3_client = boto3_session.client("s3", **self._client_kwargs)

        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            access_key=crawler.settings["AWS_ACCESS_KEY_ID"],
            secret_key=crawler.settings["AWS_SECRET_ACCESS_KEY"],
            session_token=crawler.settings["AWS_SESSION_TOKEN"],
            acl=crawler.settings["FEED_STORAGE_S3_ACL"] or None,
            endpoint_url=crawler.settings["AWS_ENDPOINT_URL"] or None,
            region_name=crawler.settings["AWS_REGION_NAME"] or None,
            max_pool_connections=_get_max_pool_connections(crawler.settings),
            upload_timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
            feed_options=feed_options,
        )

    def store(self, file: IO[bytes]) -> Deferred[None]:
        if is_asyncio_available() and is_aioboto3_available():
            return deferred_from_coro(self._store_async(file))
        return super().store(file)

    async def _store_async(self, file: IO[bytes]) -> None:
        import aioboto3  # noqa: PLC0415

        file.seek(0)
        extra_args: dict[str, Any] = {"ACL": self.acl} if self.acl else {}
        session = aioboto3.Session()
        try:
            async with session.client("s3", **self._client_kwargs) as client:
                if self._mode == "create":
                    with self._raise_if_exists():
                        await client.put_object(
                            Bucket=self.bucketname,
                            Key=self.keyname,
                            Body=file,
                            IfNoneMatch="*",
                            **extra_args,
                        )
                else:
                    await client.upload_fileobj(
                        Fileobj=file,
                        Bucket=self.bucketname,
                        Key=self.keyname,
                        ExtraArgs=extra_args,
                    )
        finally:
            file.close()

    @contextlib.contextmanager
    def _raise_if_exists(self) -> Iterator[None]:
        from botocore.exceptions import ClientError  # noqa: PLC0415

        try:
            yield
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == "PreconditionFailed":
                raise FileExistsError(
                    f"s3://{self.bucketname}/{self.keyname} already exists"
                ) from error
            raise

    def _store_in_thread(self, file: IO[bytes]) -> None:
        file.seek(0)
        extra_args: dict[str, Any] = {"ACL": self.acl} if self.acl else {}
        try:
            if self._mode == "create":
                # upload_fileobj() does not allow IfNoneMatch
                # (https://github.com/boto/boto3/issues/4366).
                with self._raise_if_exists():
                    self.s3_client.put_object(
                        Bucket=self.bucketname,
                        Key=self.keyname,
                        Body=file,
                        IfNoneMatch="*",
                        **extra_args,
                    )
            elif extra_args:
                self.s3_client.upload_fileobj(
                    Bucket=self.bucketname,
                    Key=self.keyname,
                    Fileobj=file,
                    ExtraArgs=extra_args,
                )
            else:
                self.s3_client.upload_fileobj(
                    Bucket=self.bucketname,
                    Key=self.keyname,
                    Fileobj=file,
                )
        finally:
            file.close()


class GCSFeedStorage(BlockingFeedStorage):
    """:ref:`GCS <topics-feed-storage-gcs>` storage backend."""

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        project_id: str | None,
        acl: str | None,
        *,
        feed_options: dict[str, Any] | None = None,
        timeout: float | None = None,
    ):
        self.project_id: str | None = project_id
        self.acl: str | None = acl
        self.timeout: float | None = timeout
        u = urlparse(uri)
        assert u.hostname
        self.bucket_name: str = u.hostname
        self.blob_name: str = u.path[1:]  # remove first "/"

        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            crawler.settings["GCS_PROJECT_ID"],
            crawler.settings["FEED_STORAGE_GCS_ACL"] or None,
            feed_options=feed_options,
            timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
        )

    def _store_in_thread(self, file: IO[bytes]) -> None:
        from google.api_core.exceptions import PreconditionFailed  # noqa: PLC0415
        from google.cloud.storage import Client  # noqa: PLC0415

        file.seek(0)
        try:
            client = Client(project=self.project_id)
            bucket = client.bucket(self.bucket_name)
            kwargs: dict[str, Any] = (
                {} if self.timeout is None else {"timeout": self.timeout}
            )
            blob = (
                bucket.get_blob(self.blob_name, **kwargs)
                if self._mode == "append"
                else None
            )
            if blob is None:
                upload_kwargs = (
                    {**kwargs, "if_generation_match": 0}
                    if self._mode == "create"
                    else kwargs
                )
                try:
                    bucket.blob(self.blob_name).upload_from_file(
                        file, predefined_acl=self.acl, **upload_kwargs
                    )
                except PreconditionFailed as error:
                    raise FileExistsError(
                        f"gs://{self.bucket_name}/{self.blob_name} already exists"
                    ) from error
                return
            # Appending uploads the new data as a separate object and composes
            # it with the existing one, leaving the data already stored
            # untouched. Composition resolves its sources by name, so it always
            # appends to the latest version of the blob.
            part = bucket.blob(f"{self.blob_name}.{uuid4().hex}.part")
            part.upload_from_file(file, predefined_acl=self.acl, **kwargs)
            try:
                blob.compose([blob, part], client=client, **kwargs)
            finally:
                part.delete(client=client, **kwargs)
            if self.acl:
                blob.acl.save_predefined(self.acl, client=client, **kwargs)
        finally:
            file.close()


class FTPFeedStorage(BlockingFeedStorage):
    """:ref:`FTP <feed-storage-ftp>` storage backend, which also handles
    :ref:`FTPS <feed-storage-ftps>` when *uri* uses the ``ftps`` scheme."""

    supported_modes: frozenset[str] = _FEED_MODES

    def __init__(
        self,
        uri: str,
        use_active_mode: bool = False,
        *,
        feed_options: dict[str, Any] | None = None,
        timeout: float | None = None,
    ):
        u = urlparse(uri)
        if not u.hostname:
            raise ValueError(f"Got a storage URI without a hostname: {uri}")
        self.host: str = u.hostname
        self.port: int = int(u.port or "21")
        self.username: str = u.username or ""
        self.password: str = unquote(u.password or "")
        self.path: str = u.path
        self.tls: bool = u.scheme == "ftps"
        self.use_active_mode: bool = use_active_mode
        self.timeout: float | None = timeout
        self._mode: str = _get_mode(self, feed_options, "overwrite")

    @property
    def overwrite(self) -> bool:
        warnings.warn(
            "FTPFeedStorage.overwrite is deprecated, use the mode feed option instead.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return self._mode != "append"

    @classmethod
    def from_crawler(
        cls,
        crawler: Crawler,
        uri: str,
        *,
        feed_options: dict[str, Any] | None = None,
    ) -> Self:
        return cls(
            uri,
            use_active_mode=crawler.settings.getbool("FEED_STORAGE_FTP_ACTIVE"),
            feed_options=feed_options,
            timeout=crawler.settings.getfloat("UPLOAD_TIMEOUT") or None,
        )

    def _store_in_thread(self, file: IO[bytes]) -> None:
        ftp_store_file(
            path=self.path,
            file=file,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            use_active_mode=self.use_active_mode,
            mode=self._mode,
            tls=self.tls,
            timeout=self.timeout,
        )


class FeedSlot:
    def __init__(
        self,
        storage: FeedStorageProtocol,
        uri: str,
        format: str,  # noqa: A002
        store_empty: bool,
        batch_id: int,
        uri_template: str,
        filter: ItemFilter,  # noqa: A002
        feed_options: dict[str, Any],
        spider: Spider,
        exporters: dict[str, type[BaseItemExporter]],
        settings: BaseSettings,
        crawler: Crawler,
        *,
        first_item: int = 1,
        manifest: _FeedManifest | None = None,
    ):
        self.file: IO[bytes] | None = None
        self.exporter: BaseItemExporter | None = None
        self.storage: FeedStorageProtocol = storage
        # feed params
        self.batch_id: int = batch_id
        # 1-based index of the first exported entry of this batch.
        self.first_item: int = first_item
        self.format: str = format
        self.store_empty: bool = store_empty
        self.uri_template: str = uri_template
        self.uri: str = uri
        self.filter: ItemFilter = filter
        # exporter params
        self.feed_options: dict[str, Any] = feed_options
        self.spider: Spider = spider
        self.exporters: dict[str, type[BaseItemExporter]] = exporters
        self.settings: BaseSettings = settings
        self.crawler: Crawler = crawler
        self.manifest: _FeedManifest | None = manifest
        # flags
        self.itemcount: int = 0
        self._skipped: bool = False
        self._exporting: bool = False
        self._fileloaded: bool = False
        self._started: bool = False

    async def start_exporting(self) -> None:
        if not self._fileloaded:
            if self.manifest is not None and not self._started:
                # Recorded before writing anything, so that an interrupted run
                # leaves a record of the batch that must be discarded, and of
                # whether it is safe to remove its partial target.
                self.manifest.begin_shard(
                    self.uri_template,
                    self.batch_id,
                    self.uri,
                    self.first_item,
                    pre_existing=self._target_exists(),
                )
                self._started = True
            self.file = await _maybe_coroutine(self.storage.open, self.spider)
            if "postprocessing" in self.feed_options:
                self.file = cast(
                    "IO[bytes]",
                    PostProcessingManager(
                        self.feed_options["postprocessing"],
                        self.file,
                        self.feed_options,
                    ),
                )
            self.exporter = self._get_exporter(
                file=self.file,
                format_=self.feed_options["format"],
                fields_to_export=self.feed_options["fields"],
                encoding=self.feed_options["encoding"],
                indent=self.feed_options["indent"],
                **self.feed_options["item_export_kwargs"],
            )
            self._fileloaded = True

        if not self._exporting:
            assert self.exporter
            await _maybe_coroutine(self.exporter.start_exporting)
            self._exporting = True

    def _target_exists(self) -> bool:
        """Return whether the target of the batch already exists. Local files
        can be checked before opening them; other storages cannot, and are
        treated as not pre-existing."""
        path = _local_path(self.uri)
        return path is not None and path.exists()

    def _get_exporter(
        self, file: IO[bytes], format_: str, *args: Any, **kwargs: Any
    ) -> BaseItemExporter:
        return build_from_crawler(
            self.exporters[format_], self.crawler, file, *args, **kwargs
        )

    async def finish_exporting(self) -> None:
        if self._exporting:  # pragma: no branch
            assert self.exporter
            await _maybe_coroutine(self.exporter.finish_exporting)
            self._exporting = False


class FeedExporter:
    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        exporter = cls(crawler)
        crawler.signals.connect(exporter.open_spider, signals.spider_opened)
        crawler.signals.connect(exporter._on_state_loaded, signals.spider_state_loaded)
        crawler.signals.connect(exporter._on_engine_started, signals.engine_started)
        crawler.signals.connect(exporter.close_spider, signals.spider_closed)
        crawler.signals.connect(exporter._on_state_saving, signals.spider_state_saving)
        crawler.signals.connect(exporter.item_scraped, signals.item_scraped)
        return exporter

    def __init__(self, crawler: Crawler):
        self.crawler: Crawler = crawler
        self.settings: Settings = crawler.settings
        self.feeds: dict[str, dict[str, Any]] = {}
        self.slots: list[FeedSlot] = []
        self.filters: dict[str, ItemFilter] = {}
        self.processors: dict[str, _ItemProcessor | None] = {}
        self._pending_close_tasks: list[asyncio.Task[None] | Deferred[None]] = []
        # Item export may await user-defined code (e.g. an async def
        # export_item() method), while item_scraped signals are sent
        # concurrently (see the CONCURRENT_ITEMS setting). This lock prevents
        # overlapping item export calls, which item exporters, being stateful,
        # cannot handle.
        self._export_lock = DeferredLock()

        if not self.settings["FEEDS"] and not self.settings["FEED_URI"]:
            raise NotConfigured

        # Begin: Backward compatibility for FEED_URI and FEED_FORMAT settings
        if self.settings["FEED_URI"]:
            warnings.warn(
                "The `FEED_URI` and `FEED_FORMAT` settings have been deprecated in favor of "
                "the `FEEDS` setting. Please see the `FEEDS` setting docs for more details",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )
            uri = self.settings["FEED_URI"]
            # handle pathlib.Path objects
            uri = str(uri.absolute()) if isinstance(uri, Path) else str(uri)
            feed_options = {"format": self.settings["FEED_FORMAT"]}
            self.feeds[uri] = feed_complete_default_values_from_settings(
                feed_options, self.settings, uri
            )
            self.filters[uri] = self._load_filter(feed_options)
            self.processors[uri] = self._load_processor(feed_options)
        # End: Backward compatibility for FEED_URI and FEED_FORMAT settings

        # 'FEEDS' setting takes precedence over 'FEED_URI'
        for settings_uri, feed_options in self.settings.getdict("FEEDS").items():
            # handle pathlib.Path objects
            uri = (
                str(settings_uri.absolute())
                if isinstance(settings_uri, Path)
                else str(settings_uri)
            )
            self.feeds[uri] = feed_complete_default_values_from_settings(
                feed_options, self.settings, uri
            )
            self.filters[uri] = self._load_filter(feed_options)
            self.processors[uri] = self._load_processor(feed_options)

        self.storages: dict[str, type[FeedStorageProtocol]] = self._load_components(
            "FEED_STORAGES"
        )
        self.exporters: dict[str, type[BaseItemExporter]] = self._load_components(
            "FEED_EXPORTERS"
        )
        if any(
            feed_options.get("mode") is None for feed_options in self.feeds.values()
        ):
            warnings.warn(
                "The default value of the FEED_MODE setting will change from "
                "None to 'create' in a future Scrapy version, i.e. Scrapy will "
                "stop writing feeds whose target already exists. Explicitly "
                "set FEED_MODE, or the mode feed option of every feed, to "
                "silence this warning.",
                category=ScrapyDeprecationWarning,
                stacklevel=2,
            )

        for uri, feed_options in self.feeds.items():
            if not self._storage_supported(uri, feed_options):
                raise NotConfigured
            if not self._settings_are_valid():
                raise NotConfigured
            if not self._exporter_supported(feed_options["format"]):
                raise NotConfigured

        # The manifest lives in the job directory, which is what makes a crawl
        # resumable; without a job directory there is nothing to resume.
        jobdir = _job_dir(self.settings)
        self.manifest: _FeedManifest | None = (
            _FeedManifest(Path(jobdir, _MANIFEST_FILENAME)) if jobdir else None
        )

    def open_spider(self, spider: Spider) -> None:
        self._slots_initialized = False
        for uri, feed_options in self.feeds.items():
            uri_params = self._get_uri_params(spider, feed_options["uri_params"])
            try:
                resolved_uri = apply_uri_params(uri, uri_params)
            except KeyError as exc:
                logger.error(
                    f"Feed {uri!r} could not be opened: it contains a "
                    f"placeholder for {exc}, which is not a spider "
                    f"attribute and was not provided by the uri_params "
                    f"function."
                )
                continue
            batch_id = 1
            first_item = 1
            manifest = self._manifest_for(uri)
            if manifest is not None:
                # A batch that a previous run left half-written is not a
                # batch: remove its partial file and forget it, so that the
                # resumed run starts it again instead of appending to it.
                for shard in manifest.incomplete_shards(uri):
                    if self._remove_partial_target(shard, spider):
                        manifest.forget_shard(uri, shard["batch_id"])
                # Continue after the last batch that was completely stored,
                # with the entries that follow the ones it already covers.
                batch_id = manifest.next_batch_id(uri)
                first_item = manifest.next_item_index(uri)
            self.slots.append(
                self._start_new_batch(
                    batch_id=batch_id,
                    uri=self._batch_uri(resolved_uri, batch_id, uri),
                    feed_options=feed_options,
                    spider=spider,
                    uri_template=uri,
                    first_item=first_item,
                )
            )

    def _manifest_for(self, uri_template: str) -> _FeedManifest | None:
        """Return the manifest that tracks *uri_template*, if resuming a crawl
        applies to it, i.e. if the crawl has a job directory to be resumed
        from and the feed is written in batches."""
        if self.manifest is None or not self.feeds[uri_template]["batch_item_count"]:
            return None
        return self.manifest

    @staticmethod
    def _remove_partial_target(shard: dict[str, Any], spider: Spider) -> bool:
        """Remove the file of a batch that a previous run left half-written,
        and return whether its sequence number is now free to be reused.

        The file is only removed if it is in the local filesystem, so that a
        run does not append to a corrupt file, and if the previous run created
        it, i.e. it did not exist before that run started writing it. A remote
        target, or one that already existed, is left as it is, and its
        sequence number is not reused either, so that nothing is ever written
        over it."""
        if shard.get("pre_existing"):
            return False
        uri = shard.get("uri")
        path = _local_path(uri) if isinstance(uri, str) else None
        if path is None:
            return False
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.error(f"Could not remove the partial feed file {path}: {e}")
            return False
        else:
            logger.warning(
                f"Removed the partial feed file {path} of an interrupted run; "
                "its items were not exported.",
                extra={"spider": spider},
            )
        return True

    def _batch_uri(self, resolved_uri: str, batch_id: int, uri_template: str) -> str:
        """Return the URI of the batch *batch_id* of *uri_template*.

        *resolved_uri* is the URI of batch 1, already resolved with the URI
        parameters of the spider, and is reused for it verbatim."""
        if batch_id == 1:
            return resolved_uri
        spider = self.crawler.spider
        assert spider is not None
        uri_params = self._get_uri_params(
            spider, self.feeds[uri_template]["uri_params"]
        )
        uri_params["batch_id"] = batch_id
        return apply_uri_params(uri_template, uri_params)

    async def _on_state_loaded(self, state: dict[str, Any]) -> None:
        """Update initial batch-1 slots with the correct resumed batch IDs.

        Called via the spider_state_loaded signal after SpiderState has
        populated spider.state. Safe to mutate slots here because FeedSlot
        opens its file lazily; no I/O has occurred yet.
        """
        feed_batch_ids: dict[str, int] = state.get("feed_batch_ids", {})
        if feed_batch_ids:
            spider = self.crawler.spider
            assert spider is not None
            for slot in self.slots:
                if self.manifest is not None and self.manifest.shards(
                    slot.uri_template
                ):
                    # The manifest is more precise than spider.state: it also
                    # tells complete batches from interrupted ones, and it
                    # already placed the slot at the right batch.
                    continue
                saved_id = feed_batch_ids.get(slot.uri_template, 0)
                if saved_id == 0:
                    continue
                batch_id = saved_id
                uri_params = self._get_uri_params(
                    spider, self.feeds[slot.uri_template]["uri_params"]
                )
                uri_params["batch_id"] = batch_id
                uri = apply_uri_params(slot.uri_template, uri_params)
                slot.batch_id = batch_id
                slot.uri = uri
                slot.storage = self._get_storage(uri, self.feeds[slot.uri_template])
        self._slots_initialized = True
        await self.crawler.signals.send_catch_log_async(
            signals.feed_slots_initialized, slots=self.slots
        )

    async def _on_engine_started(self) -> None:
        if not self._slots_initialized:
            self._slots_initialized = True
            await self.crawler.signals.send_catch_log_async(
                signals.feed_slots_initialized, slots=self.slots
            )

    def _on_state_saving(self, state: dict[str, Any]) -> None:
        """Persist the current batch ID for each feed into spider.state.

        Called via the spider_state_saving signal before SpiderState writes
        spider.state to disk, so the next run can resume from the right batch.
        When the feed manifest of the job directory has records for a feed, it
        is the source of truth for resuming it, and this value is only a
        fallback for job directories written by a Scrapy version that did not
        have a manifest.
        """
        feed_batch_ids = state.setdefault("feed_batch_ids", {})
        for slot in self.slots:
            feed_batch_ids[slot.uri_template] = slot.batch_id

    def _skip(self, slot: FeedSlot) -> None:
        """Report that the target of *slot* already exists, and mark *slot* so
        that nothing is written to it."""
        slot._skipped = True
        logger.error(
            f"Not writing {slot.uri} because it already exists and the feed "
            "mode is 'create'; its items are lost. To write it instead, remove "
            "the target, or set the mode feed option or the FEED_MODE setting "
            "to 'overwrite' or 'append' (-O implies 'overwrite').",
            extra={"spider": slot.spider},
        )
        assert self.crawler.stats
        self.crawler.stats.inc_value(
            f"feedexport/conflicts/{type(slot.storage).__name__}"
        )

    async def close_spider(self, spider: Spider) -> None:
        for slot in self.slots:
            self._schedule_slot_close(slot, spider)

        if self._pending_close_tasks:  # pragma: no branch
            if is_asyncio_available():
                await asyncio.wait(
                    cast("list[asyncio.Task[None]]", list(self._pending_close_tasks))
                )
            else:
                await DeferredList(
                    cast("list[Deferred[None]]", list(self._pending_close_tasks))
                )

        # Send FEED_EXPORTER_CLOSED signal
        await self.crawler.signals.send_catch_log_async(signals.feed_exporter_closed)

        if self.manifest is not None:
            self.manifest.compact()

    def _schedule_slot_close(
        self, slot: FeedSlot, spider: Spider
    ) -> asyncio.Task[None] | Deferred[None]:
        """Start closing the slot without waiting for it to finish, keeping
        track of the pending work so that it can be awaited in
        :meth:`close_spider` if it hasn't finished by then."""
        aw: asyncio.Task[None] | Deferred[None]
        coro = self._close_slot(slot, spider)
        if is_asyncio_available():
            aw = asyncio.create_task(coro)
            self._pending_close_tasks.append(aw)
            aw.add_done_callback(self._pending_close_tasks.remove)
        else:
            aw = deferred_from_coro(coro)
            self._pending_close_tasks.append(aw)
            aw.addBoth(self._untrack_pending_close_task, aw)
        return aw

    def _untrack_pending_close_task(self, result: Any, aw: Deferred[None]) -> Any:
        self._pending_close_tasks.remove(aw)
        return result

    @staticmethod
    def _get_file(slot_: FeedSlot) -> IO[bytes]:
        assert slot_.file
        if isinstance(slot_.file, PostProcessingManager):
            slot_.file.close()
            return slot_.file.file
        return slot_.file

    async def _close_slot(self, slot: FeedSlot, spider: Spider) -> None:
        if slot._skipped:
            return

        if slot.itemcount:
            # Normal case
            await slot.finish_exporting()
        elif slot.store_empty and slot.batch_id == 1:
            # Need to store the empty file
            await slot.start_exporting()
            await slot.finish_exporting()
        else:
            # In this case, the file is not stored, so no processing is required.
            return

        logmsg = f"{slot.format} feed ({slot.itemcount} items) in: {slot.uri}"
        slot_type = type(slot.storage).__name__
        self.crawler.stats.inc_value(
            f"feedexport/item_count/{slot_type}", slot.itemcount
        )
        try:
            await ensure_awaitable(slot.storage.store(self._get_file(slot)))
        except FileExistsError:
            self._skip(slot)
            self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
        except Exception:
            logger.exception(
                "Error storing %s",
                logmsg,
                extra={"spider": spider},
            )
            self.crawler.stats.inc_value(f"feedexport/failed_count/{slot_type}")
        else:
            logger.info("Stored %s", logmsg, extra={"spider": spider})
            self.crawler.stats.inc_value(f"feedexport/success_count/{slot_type}")
            if slot.manifest is not None:
                slot.manifest.complete_shard(
                    slot.uri_template, slot.batch_id, slot.itemcount
                )

        await self.crawler.signals.send_catch_log_async(
            signals.feed_slot_closed, slot=slot
        )

    def _start_new_batch(
        self,
        batch_id: int,
        uri: str,
        feed_options: dict[str, Any],
        spider: Spider,
        uri_template: str,
        first_item: int = 1,
    ) -> FeedSlot:
        """
        Redirect the output data stream to a new file.
        Execute multiple times if FEED_EXPORT_BATCH_ITEM_COUNT setting or FEEDS.batch_item_count is specified
        :param batch_id: sequence number of current batch
        :param uri: uri of the new batch to start
        :param feed_options: dict with parameters of feed
        :param spider: user spider
        :param uri_template: template of uri which contains %(batch_time)s or %(batch_id)d to create new uri
        :param first_item: 1-based index of the first exported entry of the batch
        """
        storage = self._get_storage(uri, feed_options)
        return FeedSlot(
            storage=storage,
            uri=uri,
            format=feed_options["format"],
            store_empty=feed_options["store_empty"],
            batch_id=batch_id,
            uri_template=uri_template,
            filter=self.filters[uri_template],
            feed_options=feed_options,
            spider=spider,
            exporters=self.exporters,
            settings=self.settings,
            crawler=self.crawler,
            first_item=first_item,
            manifest=self._manifest_for(uri_template),
        )

    async def item_scraped(self, item: Any, spider: Spider) -> None:
        if not self._export_lock.locked:
            # Acquiring a free lock fires the returned Deferred right away
            self._export_lock.acquire()
        else:
            await maybe_deferred_to_future(self._export_lock.acquire())
        try:
            await self._export_item(item, spider)
        finally:
            self._export_lock.release()

    async def _export_item(self, item: Any, spider: Spider) -> None:
        slots = []
        for slot in self.slots:
            if not slot.filter.accepts(item):
                slots.append(
                    slot
                )  # if slot doesn't accept item, continue with next slot
                continue

            processor = self.processors[slot.uri_template]
            for exported_item in processor(item) if processor else (item,):
                if not slot._skipped:
                    try:
                        await slot.start_exporting()
                    except FileExistsError:
                        self._skip(slot)
                if not slot._skipped:
                    assert slot.exporter
                    try:
                        await _maybe_coroutine(slot.exporter.export_item, exported_item)
                    except Exception as e:
                        e.add_note(f"Item: {exported_item!r}")
                        raise
                # Skipped items are counted, so that the following files of
                # this feed still cover the same items as those of any other
                # feed.
                slot.itemcount += 1
                # create new slot for each slot with itemcount == FEED_EXPORT_BATCH_ITEM_COUNT and close the old one
                if (
                    self.feeds[slot.uri_template]["batch_item_count"]
                    and slot.itemcount
                    >= self.feeds[slot.uri_template]["batch_item_count"]
                ):
                    uri_params = self._get_uri_params(
                        spider, self.feeds[slot.uri_template]["uri_params"], slot
                    )
                    self._schedule_slot_close(slot, spider)
                    slot = self._start_new_batch(  # noqa: PLW2901
                        batch_id=slot.batch_id + 1,
                        uri=apply_uri_params(slot.uri_template, uri_params),
                        feed_options=self.feeds[slot.uri_template],
                        spider=spider,
                        uri_template=slot.uri_template,
                        first_item=slot.first_item + slot.itemcount,
                    )
            slots.append(slot)
        self.slots = slots

    def _load_components(self, setting_prefix: str) -> dict[str, Any]:
        conf = without_none_values(
            cast("dict[str, str]", self.settings.getwithbase(setting_prefix))
        )
        d = {}
        for k, v in conf.items():
            with contextlib.suppress(NotConfigured):
                d[k] = load_object(v)
        return d

    def _exporter_supported(self, format_: str) -> bool:
        if format_ in self.exporters:
            return True
        if format_:
            logger.error("Unknown feed format: %(format)s", {"format": format_})
        else:
            logger.error(
                "Feed format not set and it could not be inferred from the "
                "feed URI; set it explicitly with the 'format' key"
            )
        return False

    def _settings_are_valid(self) -> bool:
        """
        If FEED_EXPORT_BATCH_ITEM_COUNT setting or FEEDS.batch_item_count is specified uri has to contain
        %(batch_time)s or %(batch_id)d to distinguish different files of partial output
        """
        for uri_template, values in self.feeds.items():
            if values["batch_item_count"] and not re.search(
                r"%\(batch_time\)s|%\(batch_id\)", uri_template
            ):
                logger.error(
                    "%%(batch_time)s or %%(batch_id)d must be in the feed URI (%s) if FEED_EXPORT_BATCH_ITEM_COUNT "
                    "setting or FEEDS.batch_item_count is specified and greater than 0. For more info see: "
                    "https://docs.scrapy.org/en/latest/topics/feed-exports.html#feed-export-batch-item-count",
                    uri_template,
                )
                return False
        return True

    def _storage_supported(self, uri: str, feed_options: dict[str, Any]) -> bool:
        scheme = urlparse(uri).scheme
        if scheme in self.storages or PureWindowsPath(uri).drive:
            try:
                storage = self._get_storage(uri, feed_options)
                self._check_storage_mode(uri, storage, feed_options)
                return True
            except NotConfigured as e:
                logger.error(
                    "Disabled feed storage scheme: %(scheme)s. Reason: %(reason)s",
                    {"scheme": scheme, "reason": str(e)},
                )
        else:
            logger.error("Unknown feed storage scheme: %(scheme)s", {"scheme": scheme})
        return False

    @staticmethod
    def _check_storage_mode(
        uri: str, storage: FeedStorageProtocol, feed_options: dict[str, Any]
    ) -> None:
        mode = feed_options.get("mode")
        if mode is None:
            # Every storage keeps its own historical mode.
            return
        if getattr(storage, "supported_modes", None) is None:
            logger.warning(
                f"{type(storage).__name__} does not declare which feed modes it "
                f"supports, so the mode feed option of the {uri} feed ({mode!r}) "
                "may be ignored."
            )
            return
        _check_mode(mode, storage, uri)

    def _get_storage(
        self, uri: str, feed_options: dict[str, Any]
    ) -> FeedStorageProtocol:
        """Build a storage object for the specified *uri* with the specified
        *feed_options*."""
        cls = self.storages.get(urlparse(uri).scheme, self.storages["file"])
        return build_from_crawler(cls, self.crawler, uri, feed_options=feed_options)

    def _get_uri_params(
        self,
        spider: Spider,
        uri_params_function: str | UriParamsCallableT | None,
        slot: FeedSlot | None = None,
    ) -> dict[str, Any]:
        params = {k: getattr(spider, k) for k in dir(spider)}
        utc_now = datetime.now(tz=UTC)
        params["time"] = utc_now.replace(microsecond=0).isoformat().replace(":", "-")
        params["batch_time"] = utc_now.isoformat().replace(":", "-")
        params["batch_id"] = slot.batch_id + 1 if slot is not None else 1
        uripar_function: UriParamsCallableT = (
            load_object(uri_params_function)
            if uri_params_function
            else lambda params, _: params
        )
        new_params = uripar_function(params, spider)
        return new_params if new_params is not None else params

    def _load_filter(self, feed_options: dict[str, Any]) -> ItemFilter:
        # load the item filter if declared else load the default filter class
        item_filter_class: type[ItemFilter] = load_object(
            feed_options.get("item_filter", ItemFilter)
        )
        return item_filter_class(feed_options)

    def _load_processor(self, feed_options: dict[str, Any]) -> _ItemProcessor | None:
        item_processor = feed_options.get("item_processor")
        return load_object(item_processor) if item_processor else None


def __getattr__(name: str) -> Any:  # pragma: no cover
    if name == "IFeedStorage":
        warnings.warn(
            "scrapy.extensions.feedexport.IFeedStorage is deprecated.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return _IFeedStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

from __future__ import annotations

import hashlib
import logging
import sqlite3
from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp
from typing import TYPE_CHECKING, Any, Self
from warnings import warn

from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.http import BodyFileResponse, Response
from scrapy.utils.conf import _job_dir
from scrapy.utils.request import referer_str

if TYPE_CHECKING:
    from collections.abc import Coroutine, Iterator

    from twisted.internet.defer import Deferred

    from scrapy.crawler import Crawler
    from scrapy.http.request import Request
    from scrapy.spiders import Spider


_SIZE_BYTES = 2
_BODY_FINGERPRINT_SIZE = 16


def _body_fingerprint(response: Response) -> bytes | None:
    """Return the fingerprint that identifies the body of *response*.

    Return ``None`` when there is no body to identify, i.e. for an empty
    body or for a body stored in a file, which hashing would mean reading
    whole.
    """
    if isinstance(response, BodyFileResponse) or not response.body:
        return None
    digest = hashlib.sha256(response.body).hexdigest()
    return digest[:_BODY_FINGERPRINT_SIZE].encode("ascii")


def _read_fingerprints(data: bytes) -> Iterator[bytes]:
    pos = 0
    while pos + _SIZE_BYTES <= len(data):
        size = int.from_bytes(data[pos : pos + _SIZE_BYTES], "big")
        pos += _SIZE_BYTES
        fingerprint = data[pos : pos + size]
        if len(fingerprint) < size:
            # Truncated by an unclean shutdown.
            return
        yield fingerprint
        pos += size


class BaseDupeFilter:
    """Dummy duplicate response filtering class
    (:setting:`DUPEFILTER_CLASS`) that does not filter out any response."""

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls()

    def request_seen(self, request: Request) -> bool:
        return False

    def response_seen(self, response: Response) -> bool:
        return False

    def open(self) -> Coroutine[Any, Any, None] | Deferred[None] | None:
        pass

    def close(self, reason: str) -> Coroutine[Any, Any, None] | Deferred[None] | None:
        pass

    def log(self, request: Request, spider: Spider) -> None:
        """Log that a request has been filtered"""
        warn(
            "Calling BaseDupeFilter.log() is deprecated.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )

    def log_response(self, response: Response, spider: Spider) -> None:
        """Log that a response has been filtered"""


class RFPDupeFilter(BaseDupeFilter):
    """Duplicate response filtering class (:setting:`DUPEFILTER_CLASS`) that
    filters out responses that repeat the body of a response already seen,
    using the first 16 characters of the hexadecimal SHA-256 digest of the
    body as the fingerprint.

    Job directory contents
    ======================

    .. warning:: The files that this class generates in the :ref:`job directory
        <job-dir>` are an implementation detail, and may change without a
        warning in a future version of Scrapy. Do not rely on the following
        information for anything other than debugging purposes.

    When using :setting:`JOBDIR`, seen fingerprints are tracked in a binary
    file named :file:`requests.seen` in the :ref:`job directory <job-dir>`,
    where each fingerprint is stored as its big-endian, 2-byte length followed
    by the fingerprint itself.
    """

    def __init__(
        self,
        path: str | None = None,
        debug: bool = False,
    ) -> None:
        self.file = None
        self._fingerprints: set[bytes] = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:
            self.file = Path(path, "requests.seen").open("a+b")
            self.file.seek(0)
            self._fingerprints.update(_read_fingerprints(self.file.read()))

    @property
    def fingerprints(self) -> frozenset[str]:
        warn(
            "RFPDupeFilter.fingerprints is deprecated.",
            ScrapyDeprecationWarning,
            stacklevel=2,
        )
        return frozenset(fp.decode("ascii") for fp in self._fingerprints)

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        debug = crawler.settings.getbool("DUPEFILTER_DEBUG")
        return cls(_job_dir(crawler.settings), debug)

    def request_seen(self, request: Request) -> bool:
        """Return ``False``, as requests are never filtered out."""
        return False

    def response_seen(self, response: Response) -> bool:
        """Return ``True`` if the body of *response* has been seen before."""
        fp = _body_fingerprint(response)
        if fp is None:
            return False
        if fp in self._fingerprints:
            return True
        self._fingerprints.add(fp)
        if self.file:
            self.file.write(len(fp).to_bytes(_SIZE_BYTES, "big") + fp)
        return False

    def close(self, reason: str) -> None:
        if self.file:
            self.file.close()

    def log(self, request: Request, spider: Spider) -> None:
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (referer: %(referer)s)"
            args = {"request": request, "referer": referer_str(request)}
            self.logger.debug(msg, args, extra={"spider": spider})
        elif self.logdupes:
            msg = (
                "Filtered duplicate request: %(request)s"
                " - no more duplicates will be shown"
                " (see DUPEFILTER_DEBUG to show all duplicates)"
            )
            self.logger.debug(msg, {"request": request}, extra={"spider": spider})
            self.logdupes = False

        spider.crawler.stats.inc_value("dupefilter/filtered")

    def log_response(self, response: Response, spider: Spider) -> None:
        request = response.request
        referer = referer_str(request) if request is not None else None
        if self.debug:
            self.logger.debug(
                "Filtered duplicate response: %(response)s (referer: %(referer)s)",
                {"response": response, "referer": referer},
                extra={"spider": spider},
            )
        elif self.logdupes:
            self.logger.debug(
                "Filtered duplicate response: %(response)s"
                " - no more duplicates will be shown"
                " (see DUPEFILTER_DEBUG to show all duplicates)",
                {"response": response},
                extra={"spider": spider},
            )
            self.logdupes = False

        spider.crawler.stats.inc_value("dupefilter/filtered")


class DiskDupeFilter(RFPDupeFilter):
    """Duplicate response filtering class (:setting:`DUPEFILTER_CLASS`) that
    keeps seen response fingerprints on disk, in an SQLite database, instead of
    in memory, trading some speed for a much lower memory usage on crawls that
    download a large number of unique responses.

    .. versionadded:: 2.20.0

    Job directory contents
    ======================

    .. warning:: The files that this class generates in the :ref:`job directory
        <job-dir>` are an implementation detail, and may change without a
        warning in a future version of Scrapy. Do not rely on the following
        information for anything other than debugging purposes.

    When using :setting:`JOBDIR`, seen fingerprints are tracked in an SQLite
    database named :file:`requests.seen.db` in the :ref:`job directory
    <job-dir>`.
    """

    def __init__(
        self,
        path: str | None = None,
        debug: bool = False,
    ) -> None:
        super().__init__(debug=debug)
        if path:
            self._tempdir: str | None = None
        else:
            self._tempdir = path = mkdtemp()
        self._db = sqlite3.connect(Path(path, "requests.seen.db"))
        self._db.execute("PRAGMA journal_mode=WAL").close()
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS seen (fingerprint BLOB PRIMARY KEY)"
            " WITHOUT ROWID"
        )
        self._sync_every = 0
        self._changes = 0

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        dupefilter = super().from_crawler(crawler)
        dupefilter._sync_every = crawler.settings.getint("JOBDIR_SYNC_EVERY")
        return dupefilter

    def response_seen(self, response: Response) -> bool:
        fp = _body_fingerprint(response)
        if fp is None:
            return False
        cursor = self._db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (fp,))
        if not cursor.rowcount:
            return True
        if self._sync_every:
            self._changes += 1
            if self._changes >= self._sync_every:
                self._changes = 0
                self._db.commit()
        return False

    def close(self, reason: str) -> None:
        self._db.commit()
        self._db.close()
        if self._tempdir:
            rmtree(self._tempdir)

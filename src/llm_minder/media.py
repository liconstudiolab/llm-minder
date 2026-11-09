"""Optional, bounded image/video uploads to S3-compatible object storage.

Import this module explicitly; boto3 is only needed by ``from_boto3``. Files are
streamed with a conditional single PUT (at most 5 GiB), not buffered in memory or
converted. MIME types are inferred from filenames or supplied by the caller; file
contents and model/provider support are not validated.
"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Any, TypeVar
from urllib.parse import quote, urlsplit
from uuid import uuid4

from ._internal.cleanup import wait_detached

logger = logging.getLogger(__name__)

_MAX_OBJECT_SIZE = 5 * 1024**3
_OWNER_METADATA = "llm-minder-owner"
_MEDIA_TYPE = re.compile(r"(?:image|video)/[a-z0-9!#$&^_.+\-]+", re.ASCII)
_T = TypeVar("_T")


@dataclass(frozen=True)
class UploadedMedia:
    """An uploaded object's location; a presigned ``url`` expires independently."""

    bucket: str
    key: str
    url: str
    content_type: str
    size: int


async def _join(future: asyncio.Future[Any]) -> asyncio.CancelledError | None:
    """Defer even repeated cancellation until an uninterruptible worker settles."""
    cancellation = None
    while not future.done():
        try:
            await wait_detached(future)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
    return cancellation


def _secondary_error(primary: BaseException, secondary: BaseException, action: str) -> None:
    primary.add_note(f"Media {action} failed: {secondary!r}")
    logger.warning("Media %s failed", action, exc_info=secondary)


async def _finish(future: asyncio.Future[_T]) -> _T:
    cancellation = await _join(future)
    if cancellation is not None:
        try:
            future.result()
        except BaseException as error:
            _secondary_error(cancellation, error, "operation during cancellation")
        raise cancellation
    return future.result()


def _error_code(error: BaseException) -> str:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        return str(response.get("Error", {}).get("Code", ""))
    return ""


class S3MediaUploader:
    """Async adapter around a synchronous boto3-compatible S3 client.

    ``max_concurrency`` bounds complete operations, including cancellation cleanup.
    Cancellation waits for the blocking SDK call, so configure SDK connection/read
    timeouts for your environment. SDK calls must support conditional PUT/DELETE.
    An injected client is never closed. ``from_boto3`` creates an owned client that
    is closed by ``aclose`` or the async context manager. Closing does not delete
    successful uploads; call ``delete`` explicitly before closing if desired.

    Without ``public_base_url``, URLs are presigned GET URLs. A public base must
    already serve the bucket's objects (e.g. an R2 custom domain); this class does
    not change bucket permissions or ACLs. Its path is preserved, and object keys
    are URL-escaped. The bucket name is not appended to a public base.
    """

    def __init__(
        self,
        client: Any,
        bucket: str,
        *,
        public_base_url: str | None = None,
        max_concurrency: int = 4,
        prefix: str = "llm-minder/",
        expires_in: int = 3600,
    ) -> None:
        if not isinstance(bucket, str) or not bucket.strip():
            raise ValueError("bucket must be a nonempty string")
        if type(max_concurrency) is not int or max_concurrency < 1:
            raise ValueError("max_concurrency must be a positive integer")
        if type(expires_in) is not int or not 1 <= expires_in <= 604800:
            raise ValueError("expires_in must be an integer between 1 and 604800 seconds")
        if not isinstance(prefix, str) or prefix.startswith("/") or "\\" in prefix:
            raise ValueError("prefix must be a relative S3 key prefix")
        if any(part in {".", ".."} for part in prefix.split("/")):
            raise ValueError("prefix must not contain '.' or '..' path segments")
        if any(ord(character) < 32 or ord(character) == 127 for character in prefix):
            raise ValueError("prefix must not contain control characters")
        if public_base_url is not None:
            if not isinstance(public_base_url, str) or any(
                character.isspace() or ord(character) < 32 for character in public_base_url
            ):
                raise ValueError("public_base_url must be an absolute HTTP(S) URL")
            parts = urlsplit(public_base_url)
            if (
                parts.scheme not in {"http", "https"}
                or not parts.hostname
                or parts.username is not None
                or parts.password is not None
                or parts.query
                or parts.fragment
                or "?" in public_base_url
                or "#" in public_base_url
                or "\\" in public_base_url
            ):
                raise ValueError(
                    "public_base_url must be HTTP(S), without credentials/query/fragment"
                )
            # Accessing port also validates malformed or out-of-range port numbers.
            _ = parts.port
        self._client = client
        self._bucket = bucket
        self._public_base_url = public_base_url.rstrip("/") if public_base_url else None
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._expires_in = expires_in
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._owned_client = False
        self._closing = False
        self._closed = False
        self._active = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._close_task: asyncio.Task[None] | None = None
        self._uploads: dict[str, str] = {}

    @classmethod
    def from_boto3(
        cls,
        bucket: str,
        *,
        public_base_url: str | None = None,
        max_concurrency: int = 4,
        prefix: str = "llm-minder/",
        expires_in: int = 3600,
        **client_options: Any,
    ) -> S3MediaUploader:
        """Create an owned S3 client, forwarding options to ``boto3.client``.

        Pass ``endpoint_url`` for R2/S3-compatible services and ``region_name`` as
        needed. Credentials use boto3's normal credential chain unless explicitly
        supplied; never put credentials in source code. Client creation is sync.
        """
        uploader = cls(
            None,
            bucket,
            public_base_url=public_base_url,
            max_concurrency=max_concurrency,
            prefix=prefix,
            expires_in=expires_in,
        )
        try:
            import boto3  # type: ignore[import-untyped]
        except ImportError as error:
            raise ImportError(
                "Install llm-minder[media] to use S3MediaUploader.from_boto3"
            ) from error
        uploader._client = boto3.client("s3", **client_options)
        uploader._owned_client = True
        return uploader

    def _ensure_open(self) -> None:
        if self._closing:
            raise RuntimeError("S3MediaUploader is closing or closed")

    async def __aenter__(self) -> S3MediaUploader:
        self._ensure_open()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            await self.aclose()
        except BaseException as error:
            if exc is None:
                raise
            _secondary_error(exc, error, "close")

    async def aclose(self) -> None:
        """Reject new work, join active operations, and close an owned client.

        Cancellation is deferred until closing finishes. A failed client close
        remains retryable by calling this method again.
        """
        if self._closed:
            return
        self._closing = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        close_task = self._close_task
        try:
            await _finish(close_task)
        finally:
            if close_task.done() and not self._closed:
                self._close_task = None

    async def _close(self) -> None:
        await self._idle.wait()
        if self._owned_client:
            await asyncio.to_thread(self._client.close)
        self._closed = True

    async def upload(
        self, path: str | os.PathLike[str], content_type: str | None = None
    ) -> UploadedMedia:
        """Stream one image/video file to a new key and return its usable URL.

        Explicit content types override filename inference, but must be an image
        or video MIME type without parameters. Files must remain unchanged until
        the operation finishes. Single-PUT uploads are limited to 5 GiB. Failures
        and cancellation best-effort remove only an object bearing
        this operation's ownership metadata. Cleanup failures are logged and added
        as notes to the original exception; they never hide its type.
        """
        self._ensure_open()
        source = Path(path)
        if content_type is None:
            content_type, encoding = mimetypes.guess_type(source.name)
            if encoding is not None:
                raise ValueError(
                    "Compressed filenames require an explicit image/video content_type"
                )
        if not isinstance(content_type, str) or not _MEDIA_TYPE.fullmatch(content_type.lower()):
            raise ValueError("content_type must be an image/* or video/* MIME type")
        content_type = content_type.lower()
        key = f"{self._prefix}{uuid4().hex}/{source.name}"
        if len(key.encode("utf-8")) > 1024:
            raise ValueError("The generated S3 key exceeds 1024 UTF-8 bytes")
        owner = uuid4().hex
        async with self._semaphore:
            self._ensure_open()
            self._active += 1
            self._idle.clear()
            try:
                worker = asyncio.create_task(
                    asyncio.to_thread(self._upload, source, key, owner, content_type)
                )
                cancellation = await _join(worker)
                if cancellation is not None:
                    try:
                        worker.result()
                    except BaseException as error:
                        _secondary_error(cancellation, error, "upload during cancellation")
                    else:
                        cleanup = asyncio.create_task(
                            asyncio.to_thread(self._delete_owned, key, owner)
                        )
                        await _join(cleanup)
                        try:
                            cleanup.result()
                        except BaseException as error:
                            _secondary_error(cancellation, error, "cancelled upload cleanup")
                    raise cancellation
                result = worker.result()
                self._uploads[key] = owner
                return result
            finally:
                self._active -= 1
                if not self._active:
                    self._idle.set()

    def _upload(self, source: Path, key: str, owner: str, content_type: str) -> UploadedMedia:
        # Reject pipes before opening: a blocking FIFO open cannot be cancelled.
        if not stat.S_ISREG(source.stat().st_mode):
            raise ValueError("Upload source must be a regular file")
        with source.open("rb") as body:
            info = os.fstat(body.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("Upload source must be a regular file")
            if info.st_size > _MAX_OBJECT_SIZE:
                raise ValueError("Single-PUT media uploads cannot exceed 5 GiB")
            try:
                self._client.put_object(
                    Bucket=self._bucket,
                    Key=key,
                    Body=body,
                    ContentLength=info.st_size,
                    ContentType=content_type,
                    IfNoneMatch="*",
                    Metadata={_OWNER_METADATA: owner},
                )
                if self._public_base_url is None:
                    url = self._client.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": self._bucket, "Key": key},
                        ExpiresIn=self._expires_in,
                    )
                else:
                    url = f"{self._public_base_url}/{quote(key, safe='/')}"
                if not isinstance(url, str) or not url:
                    raise ValueError("The S3 client did not return a usable presigned URL")
                return UploadedMedia(self._bucket, key, url, content_type, info.st_size)
            except BaseException as error:
                # On ambiguous network errors a PUT may already have committed.
                # HEAD plus an ownership marker distinguishes that from collisions.
                try:
                    self._delete_owned(key, owner)
                except BaseException as cleanup_error:
                    _secondary_error(error, cleanup_error, "failed upload cleanup")
                raise

    def _delete_owned(self, key: str, owner: str) -> None:
        try:
            current = self._client.head_object(Bucket=self._bucket, Key=key)
        except Exception as error:
            if _error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return
            raise
        if current.get("Metadata", {}).get(_OWNER_METADATA) != owner:
            return
        etag = current.get("ETag")
        if not isinstance(etag, str) or not etag:
            raise RuntimeError("Cannot safely delete an uploaded object without its ETag")
        self._client.delete_object(Bucket=self._bucket, Key=key, IfMatch=etag)

    async def delete(self, media: UploadedMedia) -> None:
        """Delete a successful upload from this uploader, without touching replacements.

        Unknown objects and objects from other uploaders are rejected. Repeated
        deletes are rejected once the upload has been removed from this instance.
        """
        self._ensure_open()
        if media.bucket != self._bucket or media.key not in self._uploads:
            raise ValueError("media must be a current successful upload from this uploader")
        async with self._semaphore:
            self._ensure_open()
            owner = self._uploads.get(media.key)
            if owner is None:
                raise ValueError("media has already been deleted")
            self._active += 1
            self._idle.clear()
            try:
                worker = asyncio.create_task(
                    asyncio.to_thread(self._delete_owned, media.key, owner)
                )
                try:
                    await _finish(worker)
                finally:
                    if worker.done() and not worker.cancelled() and worker.exception() is None:
                        self._uploads.pop(media.key, None)
            finally:
                self._active -= 1
                if not self._active:
                    self._idle.set()

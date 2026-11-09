"""Offline storage behavior tests using real files and blocking storage workers."""

from __future__ import annotations

import asyncio
import hashlib
import sys
import threading
from types import SimpleNamespace
from typing import Any
from urllib.parse import quote

import pytest

from llm_minder.media import S3MediaUploader


class StorageError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": 412}}


class RecordingStorage:
    """Consume file bodies in chunks, modeling objects and ambiguous PUT errors."""

    def __init__(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.started: asyncio.Queue[str] = asyncio.Queue()
        self.release = threading.Event()
        self.release.set()
        self.lock = threading.Lock()
        self.objects: dict[str, dict[str, Any]] = {}
        self.deleted: list[str] = []
        self.streams: list[Any] = []
        self.active = 0
        self.peak = 0
        self.put_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.sign_error: Exception | None = None
        self.closed = False
        self.signed: list[tuple[str, dict[str, str], int]] = []

    def put_object(self, **kwargs: Any) -> None:
        key = kwargs["Key"]
        body = kwargs["Body"]
        assert not isinstance(body, (bytes, bytearray))
        assert body.fileno() >= 0
        assert kwargs["IfNoneMatch"] == "*"
        self.streams.append(body)
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        self.loop.call_soon_threadsafe(self.started.put_nowait, key)
        try:
            assert self.release.wait(timeout=10), "test failed to release storage worker"
            digest = hashlib.sha256()
            size = 0
            while chunk := body.read(64 * 1024):
                size += len(chunk)
                digest.update(chunk)
            assert size == kwargs["ContentLength"]
            self.objects[key] = {
                "Metadata": kwargs["Metadata"],
                "ContentType": kwargs["ContentType"],
                "size": size,
                "digest": digest.hexdigest(),
                "ETag": f'"{digest.hexdigest()}"',
            }
            if self.put_error is not None:
                raise self.put_error
        finally:
            with self.lock:
                self.active -= 1

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return self.objects[Key]

    def delete_object(self, *, Bucket: str, Key: str, IfMatch: str) -> None:
        assert self.objects[Key]["ETag"] == IfMatch
        assert self.active == 0, "cleanup raced the running upload"
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append(Key)
        self.objects.pop(Key, None)

    def generate_presigned_url(
        self, ClientMethod: str, *, Params: dict[str, str], ExpiresIn: int
    ) -> str:
        if self.sign_error is not None:
            raise self.sign_error
        self.signed.append((ClientMethod, Params, ExpiresIn))
        return f"https://private.example/{Params['Key']}?expires={ExpiresIn}&signature=opaque"

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("suffix,mime", [(".png", "image/png"), (".mp4", "video/mp4")])
async def test_media_streams_real_large_file_and_preserves_metadata(tmp_path, suffix, mime):
    path = tmp_path / f"sample{suffix}"
    chunk = bytes(range(256)) * 256
    expected = hashlib.sha256()
    with path.open("wb") as stream:
        for _ in range(256):
            stream.write(chunk)
            expected.update(chunk)
    client = RecordingStorage()
    async with S3MediaUploader(client, "uploads") as uploader:
        media = await uploader.upload(path)
        stored = client.objects[media.key]
        assert media.bucket == "uploads"
        assert media.size == path.stat().st_size == 16 * 1024 * 1024
        assert media.content_type == stored["ContentType"] == mime
        assert stored["digest"] == expected.hexdigest()
        assert all(stream.closed for stream in client.streams)
        await uploader.delete(media)
        assert media.key not in client.objects
        assert client.deleted == [media.key]
    assert not client.closed, "the injected client belongs to its caller"


async def test_public_urls_escape_keys_preserve_base_path_and_use_unique_keys(tmp_path):
    path = tmp_path / "same image.png"
    path.write_bytes(b"image data")
    client = RecordingStorage()
    async with S3MediaUploader(
        client,
        "uploads",
        public_base_url="https://cdn.example/assets/",
        prefix="media space/ü/",
    ) as uploader:
        first = await uploader.upload(path)
        second = await uploader.upload(path)
    assert first.key != second.key
    for media in (first, second):
        assert media.key.startswith("media space/ü/")
        assert media.url == "https://cdn.example/assets/" + quote(media.key, safe="/")
    assert client.signed == []


@pytest.mark.parametrize("expiry", [3600, 73])
async def test_private_url_is_returned_unmodified_with_configured_expiry(tmp_path, expiry):
    path = tmp_path / "video.mp4"
    path.write_bytes(b"video data")
    client = RecordingStorage()
    options = {} if expiry == 3600 else {"expires_in": expiry}
    async with S3MediaUploader(client, "private", **options) as uploader:
        media = await uploader.upload(path)
    assert media.url == f"https://private.example/{media.key}?expires={expiry}&signature=opaque"
    assert client.signed == [("get_object", {"Bucket": "private", "Key": media.key}, expiry)]


async def test_mime_rejection_and_explicit_override_happen_before_storage(tmp_path):
    unknown = tmp_path / "upload.unknown"
    unknown.write_bytes(b"payload")
    client = RecordingStorage()
    async with S3MediaUploader(client, "uploads") as uploader:
        with pytest.raises(ValueError):
            await uploader.upload(unknown)
        with pytest.raises(ValueError):
            await uploader.upload(unknown, content_type="text/plain")
        assert client.streams == []
        media = await uploader.upload(unknown, content_type="image/png")
    assert media.content_type == "image/png"
    assert client.objects[media.key]["ContentType"] == "image/png"


async def test_missing_file_and_oversized_sparse_file_do_not_upload(tmp_path):
    path = tmp_path / "huge.mp4"
    client = RecordingStorage()
    async with S3MediaUploader(client, "uploads") as uploader:
        with pytest.raises(FileNotFoundError):
            await uploader.upload(path)
        with path.open("wb") as stream:
            stream.truncate(5 * 1024**3 + 1)
        with pytest.raises(ValueError):
            await uploader.upload(path)
    assert client.streams == []


async def test_execution_is_bounded_without_blocking_event_loop(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()
    client.release.clear()
    async with S3MediaUploader(client, "uploads", max_concurrency=2) as uploader:
        tasks = [asyncio.create_task(uploader.upload(path)) for _ in range(5)]
        try:
            await asyncio.wait_for(client.started.get(), timeout=2)
            await asyncio.wait_for(client.started.get(), timeout=2)
            await asyncio.sleep(0.02)
            assert client.active == client.peak == 2
            assert client.started.empty()
        finally:
            client.release.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
    assert not any(isinstance(result, BaseException) for result in results)
    assert client.peak == 2
    assert len(client.objects) == 5


async def test_repeated_cancellation_joins_upload_before_cleanup_and_releasing_slot(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()
    client.release.clear()
    async with S3MediaUploader(client, "uploads", max_concurrency=1) as uploader:
        cancelled = asyncio.create_task(uploader.upload(path))
        pending = None
        try:
            first_key = await asyncio.wait_for(client.started.get(), timeout=2)
            cancelled.cancel()
            await asyncio.sleep(0.02)
            cancelled.cancel()
            pending = asyncio.create_task(uploader.upload(path))
            await asyncio.sleep(0.02)
            assert not cancelled.done()
            assert client.active == 1
            assert client.deleted == []
            assert client.started.empty(), "cancellation released the concurrency slot early"
        finally:
            client.release.set()
            results = await asyncio.gather(
                *([cancelled, pending] if pending is not None else [cancelled]),
                return_exceptions=True,
            )
        assert isinstance(results[0], asyncio.CancelledError)
        assert not isinstance(results[1], BaseException)
        assert first_key in client.deleted
        assert first_key not in client.objects
        assert len(client.objects) == 1
        assert client.peak == 1
        assert all(stream.closed for stream in client.streams)


@pytest.mark.parametrize("stage", ["put", "sign"])
async def test_failure_cleans_owned_object_and_preserves_original_error(tmp_path, stage):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()
    failure = RuntimeError(f"{stage} failed")
    if stage == "put":
        client.put_error = failure
    else:
        client.sign_error = failure
    async with S3MediaUploader(client, "uploads") as uploader:
        with pytest.raises(RuntimeError) as caught:
            await uploader.upload(path)
    assert caught.value is failure
    assert len(client.deleted) == 1
    assert client.objects == {}
    assert all(stream.closed for stream in client.streams)


async def test_cleanup_failure_is_visible_without_replacing_upload_error(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()
    client.put_error = RuntimeError("upload failed")
    client.delete_error = RuntimeError("delete denied")
    async with S3MediaUploader(client, "uploads") as uploader:
        with pytest.raises(RuntimeError, match="upload failed") as caught:
            await uploader.upload(path)
    assert caught.value is client.put_error
    assert any("delete denied" in note for note in caught.value.__notes__)
    assert len(client.objects) == 1


async def test_conditional_write_failure_never_deletes_existing_object(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()

    def reject_put(**kwargs):
        client.objects[kwargs["Key"]] = {"Metadata": {"llm-minder-owner": "someone-else"}}
        raise StorageError("PreconditionFailed")

    client.put_object = reject_put
    async with S3MediaUploader(client, "uploads") as uploader:
        with pytest.raises(StorageError):
            await uploader.upload(path)
    assert len(client.objects) == 1
    assert client.deleted == []


async def test_owned_client_closes_only_after_active_upload_finishes(tmp_path, monkeypatch):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()
    client.release.clear()
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *a, **k: client))
    uploader = S3MediaUploader.from_boto3("uploads")
    upload = asyncio.create_task(uploader.upload(path))
    close = None
    try:
        await asyncio.wait_for(client.started.get(), timeout=2)
        close = asyncio.create_task(uploader.aclose())
        await asyncio.sleep(0.02)
        assert not close.done()
        assert not client.closed
        with pytest.raises(RuntimeError, match="clos"):
            await uploader.upload(path)
    finally:
        client.release.set()
        await asyncio.gather(upload, *([close] if close is not None else []))
        await uploader.aclose()
    assert client.closed
    assert len(client.objects) == 1, "closing must not delete successful uploads"


async def test_cancellation_while_queued_never_starts_storage_operation(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()
    client.release.clear()
    async with S3MediaUploader(client, "uploads", max_concurrency=1) as uploader:
        first = asyncio.create_task(uploader.upload(path))
        queued = None
        try:
            await asyncio.wait_for(client.started.get(), timeout=2)
            queued = asyncio.create_task(uploader.upload(path))
            await asyncio.sleep(0)
            queued.cancel()
            with pytest.raises(asyncio.CancelledError):
                await queued
            assert client.started.empty()
            assert client.deleted == []
        finally:
            client.release.set()
            await asyncio.gather(
                first, *([queued] if queued is not None else []), return_exceptions=True
            )
    assert len(client.streams) == len(client.objects) == 1


async def test_ambiguous_failure_does_not_delete_foreign_replacement(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"image")
    client = RecordingStorage()

    def replaced_put(**kwargs):
        client.objects[kwargs["Key"]] = {
            "Metadata": {"llm-minder-owner": "someone-else"},
            "ETag": '"replacement"',
        }
        raise RuntimeError("connection lost")

    client.put_object = replaced_put
    async with S3MediaUploader(client, "uploads") as uploader:
        with pytest.raises(RuntimeError, match="connection lost"):
            await uploader.upload(path)
    assert len(client.objects) == 1
    assert client.deleted == []


def test_from_boto3_explains_missing_optional_dependency(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ImportError, match=r"llm-minder\[media\]"):
        S3MediaUploader.from_boto3("uploads")

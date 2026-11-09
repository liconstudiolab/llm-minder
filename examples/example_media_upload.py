"""Upload real image/video files to S3 or R2, then delete the uploaded objects.

Install: uv sync --extra media
Run: uv run python examples/example_media_upload.py photo.png clip.mp4 --bucket YOUR_BUCKET

Use the standard AWS credential chain; never put credentials in this script.
For R2, also pass --endpoint-url https://ACCOUNT_ID.r2.cloudflarestorage.com --region auto.
Printed private URLs are temporary bearer credentials. Do not publish their output.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import AsyncExitStack
from dataclasses import asdict
from pathlib import Path

from llm_minder.media import S3MediaUploader


async def upload_files(args: argparse.Namespace) -> None:
    client_options = {}
    if args.endpoint_url:
        client_options["endpoint_url"] = args.endpoint_url
    if args.region:
        client_options["region_name"] = args.region
    async with S3MediaUploader.from_boto3(
        args.bucket,
        public_base_url=args.public_base_url,
        expires_in=args.expires_in,
        **client_options,
    ) as uploader:
        # Register each successful upload immediately: later failures still clean it up.
        async with AsyncExitStack() as cleanup:
            for path in args.paths:
                media = await uploader.upload(path)
                cleanup.push_async_callback(uploader.delete, media)
                print(json.dumps(asdict(media), ensure_ascii=False))
            # Consume media.url here with your actual provider client, using that
            # provider's image/video message format and a compatible model.
            # This uploader does not call a model or convert video formats.
            if args.hold_seconds:
                print(f"Keeping objects available for {args.hold_seconds:g} seconds...")
                await asyncio.sleep(args.hold_seconds)
        print("Uploaded objects deleted.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path, help="Local image/video files")
    parser.add_argument(
        "--bucket", required=True, help="Existing bucket with PUT/GET/DELETE access"
    )
    parser.add_argument("--endpoint-url", help="S3-compatible endpoint; omit for AWS S3")
    parser.add_argument("--region", help="AWS region, or auto for R2")
    parser.add_argument("--public-base-url", help="Already-configured public bucket/CDN URL")
    parser.add_argument(
        "--expires-in", type=int, default=3600, help="Private URL lifetime in seconds"
    )
    parser.add_argument(
        "--hold-seconds",
        type=float,
        default=0,
        help="Keep objects available before cleanup (default: delete immediately)",
    )
    args = parser.parse_args()
    if args.hold_seconds < 0:
        parser.error("--hold-seconds must be nonnegative")
    asyncio.run(upload_files(args))


if __name__ == "__main__":
    main()

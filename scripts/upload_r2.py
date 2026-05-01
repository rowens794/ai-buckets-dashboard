from __future__ import annotations

from datetime import datetime, timezone
import json
import mimetypes
import os
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

REQUIRED = ["CF_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]
missing_env = [k for k in REQUIRED if not os.environ.get(k)]
public_base_url = (os.environ.get("R2_PUBLIC_BASE_URL") or "").rstrip("/")
endpoint = (os.environ.get("R2_ENDPOINT") or "").rstrip("/")
if not endpoint and os.environ.get("CF_ACCOUNT_ID"):
    endpoint = f"https://{os.environ['CF_ACCOUNT_ID']}.r2.cloudflarestorage.com"

runtime_config = {
    "r2_public_base_url": public_base_url,
    "preferred_data_source": "r2" if public_base_url else "github-pages",
    "generated_at": datetime.now(timezone.utc).isoformat(),
}
(DATA / "runtime_config.json").write_text(json.dumps(runtime_config, indent=2) + "\n")

if missing_env:
    print(f"R2 upload skipped; missing env vars: {', '.join(missing_env)}")
    raise SystemExit(0)

bucket = os.environ["R2_BUCKET"]
run_date = os.environ.get("SNAPSHOT_DATE") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"),
    region_name="auto",
)

def upload(local: Path, key: str) -> None:
    content_type = mimetypes.guess_type(local.name)[0] or "application/octet-stream"
    extra = {"ContentType": content_type, "CacheControl": "public, max-age=60"}
    s3.upload_file(str(local), bucket, key, ExtraArgs=extra)
    print(f"uploaded s3://{bucket}/{key}")

files = [
    (DATA / "bucket_indexes.csv", "bucket_indexes.csv"),
    (DATA / "metadata.json", "metadata.json"),
    (DATA / "runtime_config.json", "runtime_config.json"),
]

for local, name in files:
    if not local.exists():
        raise FileNotFoundError(local)
    upload(local, f"latest/{name}")
    upload(local, f"history/{run_date}/{name}")

manifest = {
    "latest_date": run_date,
    "latest": {
        "bucket_indexes_csv": "latest/bucket_indexes.csv",
        "metadata_json": "latest/metadata.json",
        "runtime_config_json": "latest/runtime_config.json",
    },
    "history_prefix": "history/",
    "updated_at": datetime.now(timezone.utc).isoformat(),
}
manifest_path = DATA / "r2_manifest.json"
manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
upload(manifest_path, "manifest.json")
upload(manifest_path, f"history/{run_date}/manifest.json")

print("R2 upload complete")

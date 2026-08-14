import os
from pathlib import Path

from minio import Minio


def _get_minio_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    if not secret_key:
        raise ValueError("MINIO_SECRET_KEY must be set in the environment.")
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def main() -> None:
    client = _get_minio_client()
    bucket_name = os.getenv("MINIO_BUCKET", "lakehouse")
    local_data_path = os.getenv("BATCH_DATA_DIR", "/app/infra/workspace")

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Đã tạo bucket: {bucket_name}")

    base_path = Path(local_data_path)
    if not base_path.is_dir():
        raise FileNotFoundError(f"Đường dẫn không hợp lệ: {local_data_path}")

    print("Đang nạp dữ liệu batch vào MinIO...")
    uploaded_files = 0
    for file_path in sorted(base_path.glob("*.csv")):
        minio_path = f"raw_data/batch/{file_path.name}"
        client.fput_object(bucket_name, minio_path, str(file_path))
        uploaded_files += 1
        print(f"Uploaded: {file_path.name}")

    print(f"Hoàn tất upload {uploaded_files} file.")


if __name__ == "__main__":
    main()

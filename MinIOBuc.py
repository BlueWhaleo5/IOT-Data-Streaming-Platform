from minio import Minio
from minio.error import S3Error
import io


def setup_minio_detailed():
    client = Minio(
        "192.168.58.2:31000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    # Tạo 3 buckets riêng biệt cho từng layer
    buckets = ["bronze", "silver", "gold", "data-backup"]
    for bucket in buckets:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"Created bucket: {bucket}")
        else:
            print(f"Bucket {bucket} exists")
    bucket_structures = {
        "bronze": ["normal_data/", "risk_data/", "checkpoints/normal_bronze/", "checkpoints/risk_bronze/"],
        "silver": ["cleaned_data/"],
        "gold": ["daily_summary/", "device_status/", "alerts_summary/", "dashboard_overview/"],
        "data-backup": ["warehouse/"]
    }

    print("\n Creating folder structures...")
    for bucket, folders in bucket_structures.items():
        for folder_path in folders:
            try:
                client.put_object(
                    bucket_name=bucket,
                    object_name=folder_path,
                    data=io.BytesIO(b""),
                    length=0
                )
                print(f"Created folder: {bucket}/{folder_path}")
            except Exception as e:
                print(f"Folder {bucket}/{folder_path} might exist: {e}")

    print("MinIO setup completed!")

    # Kiểm tra cấu trúc tất cả buckets
    print("\nChecking all bucket structures...")

    for bucket in buckets:
        print(f"\nBucket: {bucket}")
        try:
            objects = client.list_objects(bucket, recursive=True)
            object_count = 0
            for obj in objects:
                print(f"{obj.object_name} (size: {obj.size} bytes)")
                object_count += 1
            if object_count == 0:
                print("   (No objects found)")
            else:
                print(f"   Total objects: {object_count}")
        except Exception as e:
            print(f"Error listing {bucket}: {e}")


def test_minio_connection():
    """Test kết nối MinIO"""
    print("\nTesting MinIO connection...")
    client = Minio(
        "192.168.58.2:31000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    try:
        # Test bằng cách list buckets
        buckets = client.list_buckets()
        print("MinIO connection successful!")
        print("Available buckets:")
        for bucket in buckets:
            print(f"   - {bucket.name} (created: {bucket.creation_date})")
        return True
    except Exception as e:
        print(f"MinIO connection failed: {e}")
        return False


if __name__ == "__main__":
    # Test connection trước
    if test_minio_connection():
        # Sau đó setup buckets
        setup_minio_detailed()
    else:
        print("Cannot proceed without MinIO connection")
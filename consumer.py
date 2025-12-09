# consumer_bronze_real_time.py
from kafka import KafkaConsumer
from minio import Minio
import json
import threading
import io
import pandas as pd
from datetime import datetime
import time


class BronzeConsumerRealTime:
    def __init__(self, bootstrap_servers, minio_config):
        self.minio_client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=False
        )
        self.bootstrap_servers = bootstrap_servers
        self.batch_size = 100  # Tăng từ 5 → 100
        self.batch_timeout = 10  # Tăng từ 5 → 10 giây
        self.batch_data = {'normal': [], 'risk': []}
        self.message_count = {'normal': 0, 'risk': 0}
        self.last_save_time = time.time()

    def ensure_buckets(self):
        """Đảm bảo buckets tồn tại"""
        buckets = ['bronze']
        for bucket in buckets:
            if not self.minio_client.bucket_exists(bucket):
                self.minio_client.make_bucket(bucket)
                print(f"✅ Created bucket: {bucket}")

    def save_batch_to_minio(self, topic_type):
        """Lưu batch data vào MinIO với tối ưu real-time"""
        if not self.batch_data[topic_type]:
            return

        try:
            # Convert batch to DataFrame
            df = pd.DataFrame(self.batch_data[topic_type])

            # Tạo filename với microsecond precision
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            filename = f"{topic_type}_data/stream_{timestamp}.parquet"

            # Convert to Parquet in memory
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            # Upload to MinIO
            self.minio_client.put_object(
                bucket_name="bronze",
                object_name=filename,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )

            print(f"🚀 REAL-TIME: Saved {len(self.batch_data[topic_type])} records to bronze/{filename}")

            # Update metrics
            self.message_count[topic_type] += len(self.batch_data[topic_type])

            # Clear batch
            self.batch_data[topic_type] = []

        except Exception as e:
            print(f"❌ Error saving batch to MinIO: {e}")

    def auto_save_timer(self):
        """Timer tự động lưu batch theo định kỳ"""
        while True:
            current_time = time.time()
            if current_time - self.last_save_time >= self.batch_timeout:
                for topic_type in ['normal', 'risk']:
                    if self.batch_data[topic_type]:
                        self.save_batch_to_minio(topic_type)
                self.last_save_time = current_time
            time.sleep(1)

    def consume_topic(self, topic_name, consumer_name):
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                group_id=f'{consumer_name}-optimized-group',
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,  # Tăng commit interval
                session_timeout_ms=30000,  # Tăng timeout
                heartbeat_interval_ms=10000,
                fetch_min_bytes=1024,  # Đợi ít nhất 1KB
                fetch_max_wait_ms=500,  # Max wait time
                max_poll_records=500,  # Tăng records per poll
                max_partition_fetch_bytes=1048576  # 1MB per partition
            )

            print(f"🎯 {consumer_name} REAL-TIME - Listening to: {topic_name}")
            print(f"   Batch size: {self.batch_size}, Timeout: {self.batch_timeout}s")
            print("-" * 60)

            topic_type = "risk" if "risk" in topic_name else "normal"

            for message in consumer:
                data = message.value

                # Thêm metadata với timestamp chính xác
                data['_kafka_offset'] = message.offset
                data['_kafka_partition'] = message.partition
                data['_consumed_at'] = datetime.now().isoformat()
                data['_processed_timestamp'] = int(time.time() * 1000)  # Millisecond precision

                # Thêm vào batch
                self.batch_data[topic_type].append(data)

                # Log real-time (giảm verbosity)
                original_data = data.get('original_data', {})
                if len(self.batch_data[topic_type]) % 3 == 0:  # Chỉ log mỗi 3 messages
                    print(f"📥 {consumer_name} - Batch: {len(self.batch_data[topic_type])}/{self.batch_size} | "
                          f"Device: {original_data.get('device', 'N/A')} | "
                          f"Temp: {data.get('temperature', 'N/A')}°C")

                # Lưu batch khi đủ size
                if len(self.batch_data[topic_type]) >= self.batch_size:
                    self.save_batch_to_minio(topic_type)
                    self.last_save_time = time.time()

        except Exception as e:
            print(f"❌ Error in {consumer_name}: {e}")
            import traceback
            traceback.print_exc()

    def start_consumers(self):
        """Chạy cả hai consumers với real-time optimization"""
        MINIKUBE_IP = "192.168.58.2"
        BOOTSTRAP_SERVERS = [f'{MINIKUBE_IP}:31092']

        MINIO_CONFIG = {
            'endpoint': f"{MINIKUBE_IP}:31000",
            'access_key': "minioadmin",
            'secret_key': "minioadmin"
        }

        self.minio_client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=False
        )

        # Đảm bảo buckets tồn tại
        self.ensure_buckets()

        print("🚀 BẮT ĐẦU REAL-TIME BRONZE CONSUMER")
        print("   Optimized for low latency streaming")
        print("   Batch Size: 5 messages")
        print("   Batch Timeout: 5 seconds")
        print("=" * 60)

        # Start auto-save timer thread
        timer_thread = threading.Thread(target=self.auto_save_timer, daemon=True)
        timer_thread.start()

        # Tạo threads cho mỗi consumer
        normal_thread = threading.Thread(
            target=self.consume_topic,
            args=('normal.telemetry', 'NORMAL REAL-TIME'),
            daemon=True
        )

        risk_thread = threading.Thread(
            target=self.consume_topic,
            args=('risk.telemetry', 'RISK REAL-TIME'),
            daemon=True
        )

        # Start threads
        normal_thread.start()
        risk_thread.start()

        # Metrics reporting thread
        def report_metrics():
            while True:
                time.sleep(10)
                total_messages = sum(self.message_count.values())
                print(f"\n📊 REAL-TIME METRICS - Total processed: {total_messages} messages")
                print(f"   Normal: {self.message_count['normal']} | Risk: {self.message_count['risk']}")
                print(
                    f"   Current batches - Normal: {len(self.batch_data['normal'])}, Risk: {len(self.batch_data['risk'])}")
                print("-" * 50)

        metrics_thread = threading.Thread(target=report_metrics, daemon=True)
        metrics_thread.start()

        # Giữ chương trình chạy
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping real-time consumer...")
            # Lưu các batch cuối cùng
            for topic_type in ['normal', 'risk']:
                if self.batch_data[topic_type]:
                    self.save_batch_to_minio(topic_type)
            print("✅ Real-time consumer stopped gracefully")

    def get_throughput_stats(self):
        """Lấy thống kê throughput"""
        return {
            'total_messages': sum(self.message_count.values()),
            'normal_messages': self.message_count['normal'],
            'risk_messages': self.message_count['risk'],
            'current_batch_sizes': {
                'normal': len(self.batch_data['normal']),
                'risk': len(self.batch_data['risk'])
            }
        }


def main():
    MINIKUBE_IP = "192.168.58.2"
    BOOTSTRAP_SERVERS = [f'{MINIKUBE_IP}:31092']

    MINIO_CONFIG = {
        'endpoint': f"{MINIKUBE_IP}:31000",
        'access_key': "minioadmin",
        'secret_key': "minioadmin"
    }

    consumer = BronzeConsumerRealTime(BOOTSTRAP_SERVERS, MINIO_CONFIG)

    try:
        consumer.start_consumers()
    except KeyboardInterrupt:
        print("\n👋 Real-time pipeline stopped by user")
    except Exception as e:
        print(f"💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
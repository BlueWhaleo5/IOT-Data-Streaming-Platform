import pandas as pd
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import os


class CSVRouterProducer:
    def __init__(self, bootstrap_servers, csv_file_path):
        print(f"Đang kết nối đến: {bootstrap_servers}")
        print(f"CSV file: {csv_file_path}")

        if not os.path.exists(csv_file_path):
            print(f"File {csv_file_path} không tồn tại!")
            raise FileNotFoundError(f"File {csv_file_path} không tồn tại!")

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=30000
        )
        self.csv_file_path = csv_file_path
        self.temperature_threshold = 22.7

    def determine_topic(self, temperature):
        try:
            temp_value = float(temperature)
            return "risk.telemetry" if temp_value > self.temperature_threshold else "normal.telemetry"
        except (ValueError, TypeError):
            return "normal.telemetry"

    def process_csv_and_send(self):
        try:
            print(f"Đang đọc file: {self.csv_file_path}")
            df = pd.read_csv(self.csv_file_path)
            print(f"Đã đọc {len(df)} records")
            print(f"Các cột: {list(df.columns)}")

            normal_count = 0
            risk_count = 0

            # Xử lý toàn bộ file
            total_records = len(df)
            print(f"Đang xử lý {total_records} records...")

            for index, row in df.iterrows():
                # Sử dụng tất cả các cột từ CSV
                message_data = {
                    'timestamp': datetime.now().isoformat(),
                    'row_index': index,
                    'temperature': row['temp'],
                    'risk_type': 'HIGH_TEMPERATURE' if float(row['temp']) > self.temperature_threshold else 'NORMAL',
                    'original_data': {
                        'ts': row['ts'],
                        'device': row['device'],
                        'co': float(row['co']),
                        'humidity': float(row['humidity']),
                        'light': bool(row['light']),
                        'lpg': float(row['lpg']),
                        'motion': bool(row['motion']),
                        'smoke': float(row['smoke']),
                        'temp': float(row['temp'])
                    },
                    'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                # Xác định topic dựa trên nhiệt độ
                topic = self.determine_topic(row['temp'])

                # Gửi message
                future = self.producer.send(topic, message_data)
                future.get(timeout=10)

                if topic == "normal.telemetry":
                    normal_count += 1
                else:
                    risk_count += 1

                if (index + 1) % 100 == 0:  # Log mỗi 100 records
                    print(f"Đã gửi {index + 1}/{total_records} records...")

            self.producer.flush()

            print(f"\n🎉 HOÀN THÀNH!")
            print(f"Normal records: {normal_count}")
            print(f"Risk records: {risk_count}")
            print(f"Tổng số: {normal_count + risk_count}")
            if (normal_count + risk_count) > 0:
                print(f"Tỷ lệ Risk: {(risk_count / (normal_count + risk_count)) * 100:.1f}%")

        except Exception as e:
            print(f"Lỗi: {e}")
            import traceback
            traceback.print_exc()

    def close(self):
        self.producer.close()


if __name__ == "__main__":
    # Cấu hình kết nối
    BOOTSTRAP_SERVERS = ['localhost:9092']
    CSV_FILE_PATH = '/mnt/d/Iot/iot_telemetry_data.csv'

    print("🚀 Starting CSV to Kafka Producer...")
    print(f"Temperature threshold: 22.7°C")
    print(f"CSV file: {CSV_FILE_PATH}")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")
    print("-" * 50)

    producer = CSVRouterProducer(BOOTSTRAP_SERVERS, CSV_FILE_PATH)
    try:
        producer.process_csv_and_send()
    except Exception as e:
        print(f"Lỗi: {e}")
    finally:
        producer.close()
import trino
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import time
import warnings

warnings.filterwarnings('ignore', category=UserWarning)


class RealTimeDashboard:
    def __init__(self):
        self.host = '192.168.58.2'
        self.port = 31080
        self.conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user='trino',
            catalog='hive',
            schema='iot_dashboard_fixed'
        )
        self.running = True

    def get_available_tables(self):
        """Kiểm tra tables available"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            print("Available tables:", tables)
            return tables
        except Exception as e:
            print(f"Cannot get tables: {e}")
            return []

    def get_realtime_metrics(self):
        """Lấy metrics real-time với query tối ưu"""
        tables = self.get_available_tables()
        queries = {}

        # SỬA: Query cho gold_daily_summary - thêm điều kiện ngày
        if 'gold_daily_summary' in tables:
            queries['current_stats'] = """
                SELECT 
                    COALESCE(SUM(total_records), 0) as total_records,
                    COALESCE(COUNT(DISTINCT device), 0) as active_devices,
                    COALESCE(AVG(avg_temp), 0) as avg_temperature,
                    COALESCE(SUM(high_temp_alerts), 0) as total_alerts
                FROM gold_daily_summary 
                WHERE ingestion_date = CURRENT_DATE
            """

        # SỬA: Query cho gold_device_status - fix duplicate devices
        if 'gold_device_status' in tables:
            queries['device_status'] = """
                WITH ranked_devices AS (
                    SELECT 
                        device,
                        status,
                        avg_temp,
                        last_active,
                        ROW_NUMBER() OVER (PARTITION BY device ORDER BY last_active DESC) as rn
                    FROM gold_device_status 
                    WHERE last_active IS NOT NULL
                )
                SELECT 
                    device,
                    status,
                    avg_temp as current_temperature,
                    last_active
                FROM ranked_devices
                WHERE rn = 1
                ORDER BY last_active DESC
                LIMIT 10
            """

        # SỬA: Query cho gold_alerts_summary - thêm điều kiện ngày
        if 'gold_alerts_summary' in tables:
            queries['recent_alerts'] = """
                SELECT 
                    device,
                    alert_count,
                    alert_severity,
                    ingestion_date
                FROM gold_alerts_summary 
                WHERE ingestion_date = CURRENT_DATE
                ORDER BY alert_count DESC
                LIMIT 10
            """

        # Fallback queries nếu tables streaming chưa có
        if not queries:
            queries = {
                'system_info': "SELECT 'No streaming data yet' as info",
                'tables_info': "SHOW TABLES"
            }

        results = {}
        for key, query in queries.items():
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                results[key] = df

                # Debug: hiển thị số lượng records trả về
                if not df.empty:
                    print(f"📊 {key}: {len(df)} records")

            except Exception as e:
                print(f"Query {key} failed: {e}")
                results[key] = pd.DataFrame()

        return results

    def get_grafana_metrics(self):
        """Query tối ưu cho Grafana - với sorting ASC"""
        queries = {
            'device_temperatures': """
                SELECT 
                    device,
                    avg_temp as value,
                    last_active as time
                FROM gold_device_status 
                WHERE last_active IS NOT NULL
                ORDER BY time ASC
            """,
            'daily_temperature_trend': """
                SELECT 
                    ingestion_date as time,
                    AVG(avg_temp) as avg_temperature
                FROM gold_daily_summary
                WHERE ingestion_date IS NOT NULL
                GROUP BY ingestion_date
                ORDER BY time ASC
            """,
            'active_devices_daily': """
                SELECT 
                    ingestion_date as time,
                    COUNT(DISTINCT device) as active_devices
                FROM gold_daily_summary
                WHERE ingestion_date IS NOT NULL
                GROUP BY ingestion_date
                ORDER BY time ASC
            """
        }

        results = {}
        for key, query in queries.items():
            try:
                cursor = self.conn.cursor()
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=columns)
                results[key] = df
                print(f"📈 Grafana {key}: {len(df)} records")
            except Exception as e:
                print(f"Grafana query {key} failed: {e}")
                results[key] = pd.DataFrame()

        return results

    def update_display(self):
        """Cập nhật display real-time"""
        while self.running:
            try:
                print("\n" + "=" * 60)
                print(f"REAL-TIME IOT DASHBOARD - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("=" * 60)

                metrics = self.get_realtime_metrics()

                # Hiển thị current stats - ĐÃ FIX NULL VALUES
                if 'current_stats' in metrics and not metrics['current_stats'].empty:
                    stats = metrics['current_stats'].iloc[0]
                    print(f"📈 Today's Summary:")

                    # SỬA: Sử dụng COALESCE trong query nên không cần check null
                    total_records = stats['total_records']
                    active_devices = stats['active_devices']
                    avg_temp = stats['avg_temperature']
                    total_alerts = stats['total_alerts']

                    print(f"   📊 Records: {total_records:,}")
                    print(f"   📱 Devices: {active_devices}")
                    print(f"   🌡️  Avg Temp: {avg_temp:.1f}°C")
                    print(f"   🚨 Alerts: {total_alerts}")

                # Hiển thị device status - ĐÃ FIX DUPLICATES
                if 'device_status' in metrics and not metrics['device_status'].empty:
                    print(f"\n📱 Device Status (Latest):")
                    for _, row in metrics['device_status'].iterrows():
                        status_icon = "🟢" if row.get('status') == 'ONLINE' else "🔴"
                        device_name = row.get('device', 'Unknown')
                        temperature = row.get('current_temperature', 0)
                        last_active = row.get('last_active', 'Unknown')

                        print(f"   {status_icon} {device_name}: {temperature:.1f}°C")
                        print(f"      Last active: {last_active}")

                # Hiển thị recent alerts
                if 'recent_alerts' in metrics and not metrics['recent_alerts'].empty:
                    print(f"\n🚨 Today's Alerts:")
                    for _, row in metrics['recent_alerts'].iterrows():
                        device = row.get('device', 'Unknown')
                        alerts = row.get('alert_count', 0)
                        severity = row.get('alert_severity', 'UNKNOWN')
                        print(f"   🔥 {device}: {alerts} alerts ({severity})")

                # Nếu không có data
                if all(df.empty for df in metrics.values()):
                    print("\n⏳ Waiting for streaming data...")
                    print("   Make sure:")
                    print("   1. python hive.py is running")
                    print("   2. Spark streaming is producing data")
                    print("   3. Kafka has messages")

                    # Test Grafana queries
                    print("\n🧪 Testing Grafana queries...")
                    grafana_metrics = self.get_grafana_metrics()
                    for key, df in grafana_metrics.items():
                        if not df.empty:
                            print(f"   ✅ {key}: {len(df)} records ready for Grafana")

                print(f"\n⏰ Next update in 10 seconds...")
                time.sleep(10)

            except Exception as e:
                print(f"❌ Dashboard error: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(10)

    def start_dashboard(self):
        """Bắt đầu real-time dashboard"""
        print("🚀 Starting Real-time IoT Dashboard...")
        print("🔗 Checking connection and tables...")

        # Test connection
        try:
            tables = self.get_available_tables()
            print(f"✅ Connected to Trino. Found {len(tables)} tables")
            self.update_display()
        except Exception as e:
            print(f"❌ Cannot connect to Trino: {e}")

    def stop_dashboard(self):
        """Dừng dashboard"""
        self.running = False


if __name__ == "__main__":
    dashboard = RealTimeDashboard()
    try:
        dashboard.start_dashboard()
    except KeyboardInterrupt:
        dashboard.stop_dashboard()
        print("\n🛑 Dashboard stopped.")
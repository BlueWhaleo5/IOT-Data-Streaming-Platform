# create_new_schema.py - TRINO ONLY VERSION
import trino
from minio import Minio


def check_gold_data_trino():
    """Kiểm tra Gold data bằng Trino thay vì Spark"""
    conn = trino.dbapi.connect(
        host='192.168.58.2',
        port=31080,
        user='trino',
        catalog='hive',
        schema='iot_dashboard_fixed'
    )
    cursor = conn.cursor()

    print("🔍 Checking Gold Data via Trino...")

    try:
        # Kiểm tra Daily Summary
        cursor.execute("SELECT COUNT(*) as total_records FROM gold_daily_summary")
        daily_count = cursor.fetchone()[0]
        print(f"📊 Gold Daily Summary: {daily_count} records")

        if daily_count > 0:
            cursor.execute("SELECT * FROM gold_daily_summary LIMIT 5")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            print("Sample data:")
            for row in data:
                print(f"  {dict(zip(columns, row))}")

        # Kiểm tra Device Status
        cursor.execute("SELECT COUNT(*) as device_count FROM gold_device_status")
        status_count = cursor.fetchone()[0]
        print(f"📱 Gold Device Status: {status_count} devices")

        if status_count > 0:
            cursor.execute("SELECT * FROM gold_device_status")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            print("Device Status:")
            for row in data:
                print(f"  {dict(zip(columns, row))}")

        # KIỂM TRA THÊM: Alerts Summary
        cursor.execute("SELECT COUNT(*) as alert_count FROM gold_alerts_summary")
        alert_count = cursor.fetchone()[0]
        print(f"🚨 Gold Alerts Summary: {alert_count} alerts")

        if alert_count > 0:
            cursor.execute("SELECT * FROM gold_alerts_summary LIMIT 5")
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            print("Alerts sample:")
            for row in data:
                print(f"  {dict(zip(columns, row))}")

    except Exception as e:
        print(f"❌ Trino query error: {e}")


def create_fresh_tables():
    """Tạo tables mới bằng Trino"""
    conn = trino.dbapi.connect(
        host='192.168.58.2',
        port=31080,
        user='trino',
        catalog='hive',
        schema='default'
    )
    cursor = conn.cursor()

    # Tạo schema mới
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS hive.iot_dashboard_fixed")
        print("✅ Created schema: iot_dashboard_fixed")
    except Exception as e:
        print(f"Schema may exist: {e}")

    cursor.execute("USE hive.iot_dashboard_fixed")

    # Tạo tables với schema đầy đủ
    tables_config = {
        'gold_daily_summary': {
            'location': 's3a://gold/daily_summary',
            'schema': '''
                ingestion_date DATE,
                device VARCHAR,
                avg_temp DOUBLE,
                max_temp DOUBLE,
                min_temp DOUBLE,
                avg_humidity DOUBLE,
                total_records BIGINT,
                high_temp_alerts BIGINT,
                motion_events BIGINT,
                light_events BIGINT
            '''
        },
        'gold_device_status': {
            'location': 's3a://gold/device_status',
            'schema': '''
                device VARCHAR,
                status VARCHAR,
                avg_temp DOUBLE,
                total_readings BIGINT,
                last_active TIMESTAMP,
                total_alerts BIGINT
            '''
        },
        # THÊM TABLE MỚI - gold_alerts_summary
        'gold_alerts_summary': {
            'location': 's3a://gold/alerts_summary',
            'schema': '''
                device VARCHAR,
                alert_count BIGINT,
                alert_severity VARCHAR,
                ingestion_date DATE
            '''
        }
    }

    for table_name, config in tables_config.items():
        try:
            # Drop nếu tồn tại
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"🧹 Dropped existing table: {table_name}")

            create_sql = f"""
            CREATE TABLE {table_name} (
                {config['schema']}
            )
            WITH (
                format = 'PARQUET',
                external_location = '{config['location']}'
            )
            """
            cursor.execute(create_sql)
            print(f"✅ Created {table_name}")

            # Test query
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   Initial data count: {count}")

        except Exception as e:
            print(f"❌ Error with {table_name}: {e}")

    cursor.close()
    print("🎉 All tables created in iot_dashboard_fixed!")


def check_table_schemas():
    """Kiểm tra schema của các tables"""
    conn = trino.dbapi.connect(
        host='192.168.58.2',
        port=31080,
        user='trino',
        catalog='hive',
        schema='iot_dashboard_fixed'
    )
    cursor = conn.cursor()

    tables = ['gold_daily_summary', 'gold_device_status', 'gold_alerts_summary']

    for table in tables:
        try:
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            print(f"\n📋 Schema of {table}:")
            for col in columns:
                print(f"   {col[0]:20} {col[1]}")
        except Exception as e:
            print(f"❌ Cannot describe {table}: {e}")


if __name__ == "__main__":
    # Tạo tables trước
    create_fresh_tables()

    # Kiểm tra schema
    print("\n" + "=" * 50)
    check_table_schemas()

    # Kiểm tra data sau
    print("\n" + "=" * 50)
    check_gold_data_trino()
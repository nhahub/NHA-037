from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import json
import logging
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import sqlalchemy
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)

load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SENSOR_DATA")
timeout = 5000  # Reduced to 5 seconds for faster completion

# Alert Thresholds
TEMP_THRESHOLD = 45.0
HUMIDITY_THRESHOLD = 85.0
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "es.mohamed.eliwa2023@alexu.edu.eg")


def start_pipline():
    log.info("pipline started!")


def extract_data(**context):
    """
    Consumes all unread messages from Kafka for this consumer group.
    """
    log.info(f"Connecting to Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    data = []

    try:
        # Create a consumer with optimized settings
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="latest",  
            group_id="batch-processor-group",
            consumer_timeout_ms=timeout, 
            max_poll_records=500,  
            fetch_min_bytes=1,  # Don't wait for data to accumulate
            fetch_max_wait_ms=500,  # Wait max 500ms for fetch_min_bytes
            enable_auto_commit=True,  # Auto-commit offsets
            auto_commit_interval_ms=5000,  # Commit every 5 seconds
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        log.info("Starting message consumption...")
        message_count = 0
        start_time = datetime.now()
        max_duration_seconds = 60  # Maximum 60 seconds for extraction

        for message in consumer:
            data.append(message.value)
            message_count += 1

            if message_count % 100 == 0:
                log.info(f"Consumed {message_count} messages so far...")

            # Stop if we've been consuming for too long
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > max_duration_seconds:
                log.warning(
                    f"Stopping consumption after {elapsed:.1f}s (max: {max_duration_seconds}s)"
                )
                break

        consumer.close()

        # Log results and push to XCom
        log.info(f"Data extracted successfully! Total records: {len(data)}")
        if not data:
            log.warning("No new data found in Kafka topic.")

        context["ti"].xcom_push(key="data", value=data)

    except Exception as e:
        log.exception(f"Error extracting data from Kafka: {e}")
        raise


def process_data(**context):
    data = context["ti"].xcom_pull(key="data", task_ids="extract")

    if not data:
        log.warning("No data received from extract task! Pushing empty result.")
        context["ti"].xcom_push(key="cleaned_data", value=[])
        return

    log.info(f"Processing {len(data)} records")
    df = pd.DataFrame(data)

    log.info(f"DataFrame columns: {df.columns.tolist()}")
    log.info(f"DataFrame shape: {df.shape}")

    df = df[df["sensor_id"] > 0]
    df = df.dropna()

    df = df[
        (df["temperature"] <= 100) & (df["temperature"] >= -25)
    ]  # drop the wrong values
    context["ti"].xcom_push(key="cleaned_data", value=df.to_dict(orient="records"))


def create_table():
    """Create MySQL table if it doesn't exist"""
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://airflow:airflow@mysql/airflow_db"
    )

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sensor_readings(
        id INT AUTO_INCREMENT PRIMARY KEY,
        sensor_id INT,
        timestamp VARCHAR(255),
        temperature FLOAT,
        humidity FLOAT,
        INDEX idx_timestamp (timestamp),
        INDEX idx_sensor_id (sensor_id)
    )
    """

    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(create_table_sql))

    log.info("Table created successfully!")


def load_data(**context):
    df = pd.DataFrame(context["ti"].xcom_pull(key="cleaned_data", task_ids="transform"))
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://airflow:airflow@mysql/airflow_db"
    )

    avg_df = df.groupby("sensor_id", as_index=False)[["temperature", "humidity"]].mean()

    avg_df.rename(
        columns={"temperature": "avg_temperature", "humidity": "avg_humidity"},
        inplace=True,
    )

    avg_df.to_sql("sensor_avg", con=engine, if_exists="append", index=False)
    df.to_sql("sensor_readings", con=engine, if_exists="append", index=False)


def check_alerts(**context):
    """
    Check for alert conditions and return alert details
    """
    df = pd.DataFrame(context["ti"].xcom_pull(key="cleaned_data", task_ids="transform"))

    if df.empty:
        log.info("No data to check for alerts")
        return None

    alerts = []

    temp_alerts = df[df["temperature"] > TEMP_THRESHOLD]
    for _, row in temp_alerts.iterrows():
        alerts.append(
            {
                "type": "High Temperature",
                "sensor_id": row["sensor_id"],
                "value": row["temperature"],
                "threshold": TEMP_THRESHOLD,
                "timestamp": row["timestamp"],
            }
        )

    humidity_alerts = df[df["humidity"] > HUMIDITY_THRESHOLD]
    for _, row in humidity_alerts.iterrows():
        alerts.append(
            {
                "type": "High Humidity",
                "sensor_id": row["sensor_id"],
                "value": row["humidity"],
                "threshold": HUMIDITY_THRESHOLD,
                "timestamp": row["timestamp"],
            }
        )

    if alerts:
        log.warning(f"Found {len(alerts)} alert conditions!")
        context["ti"].xcom_push(key="alerts", value=alerts)
        return alerts
    else:
        log.info("No alert conditions detected")
        return None


def send_alert_email(**context):
    """
    Send email with alert details using SMTP directly
    """
    alerts = context["ti"].xcom_pull(key="alerts", task_ids="check_alerts")

    if not alerts:
        log.info("No alerts to send")
        return

    # Build email body
    email_body = "<h2>IoT Sensor Alert Report</h2>\n"
    email_body += f"<p><strong>Total Alerts:</strong> {len(alerts)}</p>\n"
    email_body += "<table border='1' cellpadding='5' cellspacing='0'>\n"
    email_body += "<tr><th>Alert Type</th><th>Sensor ID</th><th>Value</th><th>Threshold</th><th>Timestamp</th></tr>\n"

    for alert in alerts:
        email_body += f"<tr>"
        email_body += f"<td>{alert['type']}</td>"
        email_body += f"<td>{alert['sensor_id']}</td>"
        email_body += f"<td>{alert['value']:.2f}</td>"
        email_body += f"<td>{alert['threshold']:.2f}</td>"
        email_body += f"<td>{alert['timestamp']}</td>"
        email_body += f"</tr>\n"

    email_body += "</table>"

    # Send email directly using SMTP
    try:
        smtp_host = os.getenv("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("AIRFLOW__SMTP__SMTP_PORT", "587"))
        smtp_user = os.getenv("AIRFLOW__SMTP__SMTP_USER")
        smtp_password = os.getenv("AIRFLOW__SMTP__SMTP_PASSWORD")
        smtp_from = os.getenv("AIRFLOW__SMTP__SMTP_MAIL_FROM", smtp_user)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"IoT Sensor Alert - {datetime.now().strftime('%Y-%m-%d')}"
        msg["From"] = smtp_from
        msg["To"] = ALERT_EMAIL

        html_part = MIMEText(email_body, "html")
        msg.attach(html_part)

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_from, [ALERT_EMAIL], msg.as_string())

        log.info(
            f"Successfully sent alert email to {ALERT_EMAIL} with {len(alerts)} alerts"
        )
    except Exception as e:
        log.error(f"Failed to send alert email: {e}")
        raise


default_args = {
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:
    start = PythonOperator(task_id="start", python_callable=start_pipline)

    extract = PythonOperator(task_id="extract", python_callable=extract_data)

    transform = PythonOperator(task_id="transform", python_callable=process_data)

    create_table_task = PythonOperator(
        task_id="create_mysql_table", python_callable=create_table
    )

    load2 = PythonOperator(task_id="load_data", python_callable=load_data)

    check_alerts_task = PythonOperator(
        task_id="check_alerts", python_callable=check_alerts
    )

    send_alert_task = PythonOperator(
        task_id="send_alert_email", python_callable=send_alert_email
    )

    (
        start
        >> extract
        >> transform
        >> create_table_task
        >> load2
        >> check_alerts_task
        >> send_alert_task
    )

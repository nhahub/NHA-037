from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import json
import logging
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd
import sqlalchemy

log = logging.getLogger(__name__)

load_dotenv()
KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SENSOR_DATA")
timeout = 10000

# Alert Thresholds
TEMP_THRESHOLD = 45.0
HUMIDITY_THRESHOLD = 85.0
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "es-mohamed.bassam2027@alexu.edu.eg")


def start_pipline():
    log.info("pipline started!")


def extract_data(**context):
    """
    Consumes all unread messages from Kafka for this consumer group.
    """
    log.info(f"Connecting to Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")
    data = []

    try:
        # Create a consumer with a unique group_id
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset="earliest",
            group_id="batch-processor-group",
            consumer_timeout_ms=timeout,  # Stops iterating after 10s of no messages
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        # Consume all available messages
        for message in consumer:
            data.append(message.value)

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
        log.error("No data received from extract task!")
        # raise ValueError("No data to process")

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

    avg_df.to_sql("sensor_avg", con=engine, if_exists="replace", index=False)
    df.to_sql("sensor_readings", con=engine, if_exists="replace", index=False)


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
    Send email with alert details
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

    context["ti"].xcom_push(key="email_body", value=email_body)
    log.info(f"Prepared alert email with {len(alerts)} alerts")


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

    email_alert = EmailOperator(
        task_id="email_alert",
        to=ALERT_EMAIL,
        subject="IoT Sensor Alert - {{ ds }}",
        html_content="{{ ti.xcom_pull(task_ids='send_alert_email', key='email_body') }}",
    )

    start >> extract >> transform >> create_table_task >> load2 >> check_alerts_task

  
    check_alerts_task >> send_alert_task >> email_alert

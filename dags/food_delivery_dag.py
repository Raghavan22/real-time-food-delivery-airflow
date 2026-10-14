
import csv
import json
import logging
from pathlib import Path


from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
from airflow.sdk import DAG, task_group

DATA_FILE = Path("/opt/airflow/data/orders.csv")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

def validate_order():
    with DATA_FILE.open(mode="r", encoding="utf-8-sig", newline="") as file:
        orders = list(csv.DictReader(file))

    if not orders:
        raise ValueError("No orders found in orders.csv")

    required_fields = {
        "order_id",
        "customer_id",
        "restaurant_id",
        "amount",
        "delivery_status",
    }

    order_ids = set()

    for order in orders:
        if not required_fields.issubset(order.keys()):
            raise ValueError("CSV is missing required columns")

        if not order["order_id"].strip():
            raise ValueError("Order ID cannot be empty")

        if order["order_id"] in order_ids:
            raise ValueError(f"Duplicate order ID: {order['order_id']}")

        order_ids.add(order["order_id"])

        if float(order["amount"]) <= 0:
            raise ValueError(f"Invalid amount for {order['order_id']}")

    print(f"Successfully validated {len(orders)} orders")





def process_order():
    with DATA_FILE.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        orders = list(csv.DictReader(file))
        fieldnames = list(orders[0].keys()) if orders else []

    for order in orders:
        status = order["delivery_status"].strip().upper()

        if status == "PLACED":
            order["delivery_status"] = "PREPARING"
            print(f"{order['order_id']}: PLACED -> PREPARING")

    with DATA_FILE.open(
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)

    preparing_order_ids = [
        order["order_id"]
        for order in orders
        if order["delivery_status"].strip().upper() == "PREPARING"
    ]

    print(f"Orders sent to tracking: {preparing_order_ids}")

    return preparing_order_ids




def track_delivery(ti):
    preparing_order_ids = ti.xcom_pull(
        task_ids="order_processing.process_order"
    )

    if not preparing_order_ids:
        print("No orders are currently being prepared.")
        return

    with DATA_FILE.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        orders = list(csv.DictReader(file))

    orders_by_id = {
        order["order_id"]: order
        for order in orders
    }

    print("=== XCom Delivery Tracking Report ===")

    for order_id in preparing_order_ids:
        order = orders_by_id.get(order_id)

        if order:
            print(
                f"{order_id}: "
                f"Status = {order['delivery_status']}"
            )

def complete_demo_delivery():
    with DATA_FILE.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        orders = list(csv.DictReader(file))
        fieldnames = list(orders[0].keys()) if orders else []

    target_order_id = "ORD001"
    updated = False

    for order in orders:
        if order["order_id"] == target_order_id:
            status = order["delivery_status"].strip().upper()

            if status == "PREPARING":
                order["delivery_status"] = "DELIVERED"
                updated = True
                print(
                    f"{target_order_id}: "
                    "PREPARING -> DELIVERED"
                )
            else:
                print(
                    f"{target_order_id} cannot be completed. "
                    f"Current status: {status}"
                )
            break

    if not updated and not any(
        order["order_id"] == target_order_id for order in orders
    ):
        print(f"Order not found: {target_order_id}")

    if updated:
        with DATA_FILE.open(
            mode="w",
            encoding="utf-8",
            newline=""
        ) as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(orders)

        print("Demo delivery completed successfully.")


@task_group(group_id="order_processing")
def order_processing_group():

    validate = PythonOperator(
        task_id="validate_order",
        python_callable=validate_order,
    )

    process = PythonOperator(
        task_id="process_order",
        python_callable=process_order,
    )

    validate >> process


@task_group(group_id="delivery_tracking")
def delivery_tracking_group():

    choose_path = BranchPythonOperator(
        task_id="choose_tracking_path",
        python_callable=choose_tracking_path,
    )

    no_orders = EmptyOperator(
        task_id="no_orders_found",
    )

    track = PythonOperator(
        task_id="track_delivery",
        python_callable=track_delivery,
    )

    complete_delivery = PythonOperator(
        task_id="complete_demo_delivery",
        python_callable=complete_demo_delivery,
    )

    choose_path >> track
    choose_path >> no_orders

    track >> complete_delivery

def choose_tracking_path(ti):
    preparing_order_ids = ti.xcom_pull(
        task_ids="order_processing.process_order"
    )

    if preparing_order_ids:
        print("Preparing orders found. Starting tracking.")
        return "delivery_tracking.track_delivery"

    print("No preparing orders found.")
    return "delivery_tracking.no_orders_found"


def generate_order_summary():
    with DATA_FILE.open(
        mode="r",
        encoding="utf-8-sig",
        newline=""
    ) as file:
        orders = list(csv.DictReader(file))

    status_counts = {}
    total_amount = 0.0

    for order in orders:
        status = order["delivery_status"].strip().upper()
        status_counts[status] = status_counts.get(status, 0) + 1
        total_amount += float(order["amount"])

    summary = {
        "total_orders": len(orders),
        "status_counts": status_counts,
        "total_amount": round(total_amount, 2),
        "generated_at": datetime.now().isoformat(),
    }

    report_path = Path("/opt/airflow/reports/order_summary.json")
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with report_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=4)

    logging.info("Order summary report created: %s", report_path)
    logging.info("Order summary: %s", summary)


def notify_dag_success(context):
    dag_run = context.get("dag_run")
    logging.info(
        "SUCCESS: DAG %s completed. Run ID: %s",
        dag_run.dag_id if dag_run else "unknown",
        dag_run.run_id if dag_run else "unknown",
    )


def notify_dag_failure(context):
    dag_run = context.get("dag_run")
    logging.error(
        "FAILURE: DAG %s failed. Run ID: %s",
        dag_run.dag_id if dag_run else "unknown",
        dag_run.run_id if dag_run else "unknown",
    )

with DAG(
    dag_id="food_delivery_pipeline",
    default_args=default_args,
    description="Real-Time Food Delivery and Order Tracking System",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "order-tracking"],
) as dag:
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    
    start_order = EmptyOperator(
        task_id="start_order"
    )

    wait_for_orders = FileSensor(
        task_id="wait_for_orders_file",
        filepath="orders.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=300,
        mode="reschedule",
    )

    # Step 4: TaskGroups
    order_processing = order_processing_group()
    delivery_tracking = delivery_tracking_group()


    summary_task = PythonOperator(
        task_id="generate_order_summary",
        python_callable=generate_order_summary,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    finish_order = EmptyOperator(
        task_id="finish_order",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Dependencies
    start_order >> wait_for_orders >> order_processing
    order_processing >> delivery_tracking
    delivery_tracking >> summary_task >> finish_order
from prefect import flow, task
from customers_etl import main


@task
def run_customers_etl():
    try:
        main()
    except Exception as e:
        print(f"An error occurred while running customers ETL: {e}")


@flow(name="Customers flow")
def customers_flow():
    run_customers_etl()


if __name__ == "__main__":
    customers_flow()

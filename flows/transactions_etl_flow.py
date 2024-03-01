from prefect import flow, task
from transactions_etl import main


@task
def run_transactions_etl():
    try:
        main()
    except Exception as e:
        print(f"An error occurred while running transactions ETL: {e}")


@flow(name="Transactions flow")
def transactions_flow():
    run_transactions_etl()


if __name__ == "__main__":
    transactions_flow()

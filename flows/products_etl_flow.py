from prefect import flow, task
from products_etl import main


@task
def run_products_etl():
    try:
        main()
    except Exception as e:
        print(f"An error occurred while running products ETL: {e}")


@flow(name="Products flow")
def products_flow():
    run_products_etl()


if __name__ == "__main__":
    products_flow()

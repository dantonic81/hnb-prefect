from prefect import flow, task
from erasure_requests_etl import main


@task
def run_erasure_requests_etl():
    try:
        main()
    except Exception as e:
        print(f"An error occurred while running erasure requests ETL: {e}")


@flow(name="Erasure requests flow")
def erasure_requests_flow():
    run_erasure_requests_etl()


if __name__ == "__main__":
    erasure_requests_flow()

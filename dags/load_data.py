from airflow.decorators import dag, task
from datetime import datetime
from railbe.raildb import RailDB


@dag(
    schedule_interval = "@hourly",
    start_date = datetime(2024, 11, 1),
    tags = ["rail", "extract"],
    catchup=False
)
def load_data():
    @task
    def extract():
        # Set up database
        db = RailDB(dbname = "rail_with_dag.db")
        return db

    db = extract()

    @task
    def update(db):
        # Add station data
        db.update_stations()

        # Add liveboard data
        db.update_liveboard()
        return db

    db = update(db)

    @task
    def close(db):
        # Close connection
        db.close()
        return db

    db = close(db)

load_data()
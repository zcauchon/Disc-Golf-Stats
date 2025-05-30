import dlt
from dlt.destinations import postgres
from dlt.sources.sql_database import sql_database

def load_entire_database() -> None:
    """Use the sql_database source to completely load all tables in a database"""
    pipeline = dlt.pipeline(
        pipeline_name="pdga",
        destination="postgres",
        dataset_name="pdga_stg" # change this for target schema
    )

    # By default the sql_database source reflects all tables in the schema
    # The database credentials are sourced from the `.dlt/secrets.toml` configuration
    source = sql_database(schema="PDGA_STG") # change this for source schema

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")
    print(info)

if __name__ == "__main__":
    load_entire_database()
import os
import dotenv

dotenv.load_dotenv()
from kaggle.api.kaggle_api_extended import KaggleApi
import typer
import pathlib
import pandas as pd
from google.cloud import bigquery

app = typer.Typer()


def get_nan_columns(data):
    """
    Identify columns with NaN values and return a DataFrame with those columns and their NaN counts.

    Parameters:
    - data: pandas DataFrame

    Returns:
    - nan_df: DataFrame containing columns with NaN values and their counts
    """

    nan_counts = data.isna().sum()
    non_zero_nan_counts = nan_counts[nan_counts > 0]

    return non_zero_nan_counts


def get_sales_data(location):
    df = pd.read_csv(location, encoding="unicode_escape", parse_dates=["ORDERDATE"])
    df.rename(columns={"MSRP": "ManufacturerSuggestedPrice"}, inplace=True)
    df.columns = df.columns.str.lower()
    return df


@app.command()
def check():
    if (
        os.environ.get("KAGGLE_USERNAME") is None
        or os.environ.get("KAGGLE_KEY") is None
    ):
        typer.echo(
            "Create a .env file with KAGGLE_USERNAME and KAGGLE_KEY env variables"
        )
    else:
        typer.echo("KAGGLE_USERNAME and KAGGLE_KEY env vars are available ✅")


@app.command()
def download_sales_data():
    folder = pathlib.Path("data/raw_sales")
    folder.mkdir(exist_ok=True, parents=True)

    kaggle_api = KaggleApi()
    kaggle_api.authenticate()
    kaggle_api.dataset_download_file(
        dataset="kyanyoga/sample-sales-data",
        file_name="sales_data_sample.csv",
        path="data/raw_sales",
    )
    location = "data/raw_sales/sales_data_sample.csv"
    if pathlib.Path(location).is_file():
        typer.secho(
            "Downloaded dataset at data/raw_sales/sales_data_sample.csv", fg="green"
        )
        df = get_sales_data(location)
        typer.secho(f"Total Columns: {df.shape[1]}")
        typer.secho(f"Total Rows: {df.shape[0]}")
    else:
        typer.secho("Error Downloading file", fg="red")


@app.command()
def sanity_check():
    location = "data/raw_sales/sales_data_sample.csv"
    if pathlib.Path(location).is_file():
        df = get_sales_data(location)
        typer.secho(f"Total Columns: {df.shape[1]}")
        typer.secho(f"Total Rows: {df.shape[0]}")
        typer.secho("\nLooking for Missing values", fg="green")
        typer.secho("nan_df = data[data.isna().any(axis=1)]")
        nan_df = df[df.isna().any(axis=1)]

        if nan_df.empty:
            typer.secho("No NULL values found")
        else:
            typer.secho("Null Values Found", fg="red")
            typer.secho(get_nan_columns(nan_df))
        typer.secho("\nLooking for Duplicate records", fg="green")
        typer.secho("data.duplicated( keep='first').sum()")
        if df.duplicated(keep="first").sum() == 0:
            typer.secho("No Duplicate record found")
    else:
        typer.secho(f"File not found at location {location}", fg="red")


@app.command()
def load_to_bigquery(
    dataset: str, table: str, location: str, service_account_path: pathlib.Path
):
    client = bigquery.Client.from_service_account_json(service_account_path)
    df = get_sales_data(location="data/raw_sales/sales_data_sample.csv")
    # Check if the dataset exists, create if it doesn't
    dataset_ref = client.dataset(dataset)
    try:
        client.get_dataset(dataset_ref)
    except Exception as e:
        typer.echo(f"Dataset {dataset} not found. Creating dataset.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset=dataset, exists_ok=True)
    
    # Check if the table exists, create if it doesn't
    table_ref = dataset_ref.table(table)
    try:
        client.get_table(table_ref)
    except Exception as e:
        typer.echo(f"Table {table} not found. Creating table.")
        schema = [
            bigquery.SchemaField(name, dtype)
            for name, dtype in zip(
                df.columns,
                df.dtypes.map(str)
                .replace("object", "STRING")
                .replace("datetime64[ns]", "TIMESTAMP")
                .replace("float64", "FLOAT64")
                .replace("int64", "INT64"),
            )
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["country"]
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="orderdate"
        ) 
        client.create_table(table)
    
    # Load data into the table with truncation
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="orderdate"
        ) 
    job_config.clustering_fields = ["country"]
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()



if __name__ == "__main__":
    app()

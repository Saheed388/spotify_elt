from google.cloud import bigquery
import os

# Set your GCS bucket and CSV file details
bucket_name = os.getenv("GCP_GCS_BUCKET", "spotify_dat")
file_name = "saeed_cloud_spotify_songs_data.csv"
dataset_name = "spotify_data"
table_name = "spoty_streaming_data"
project_id = "alt-school-project-386517"

# Initialize the BigQuery client
client = bigquery.Client()

# Define the GCS URI of the CSV file
uri = f"gs://{bucket_name}/{file_name}"

# Define the BigQuery table ID
table_id = f"{project_id}.{dataset_name}.{table_name}"

# Configure the job to load data from GCS into BigQuery
job_config = bigquery.LoadJobConfig(
    autodetect=True,  # Automatically detect schema
    skip_leading_rows=1,  # Skip header row
    source_format=bigquery.SourceFormat.CSV,
)

# Start the load job
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)

# Wait for the job to complete
load_job.result()

# Check if the job succeeded
if load_job.state == "DONE":
    print("Data loaded successfully into BigQuery.")
else:
    print("Error loading data into BigQuery:", load_job.errors)

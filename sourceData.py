
from google.cloud import bigquery as bq
from google.cloud import secretmanager as sm


# Retrieves the list of datasets from the BigQuery project
def getDatasets():
    client = bq.Client(project='footballdataproject-458811')  # Instantiate BigQuery client
    datasets = list(client.list_datasets())  # List all datasets in the project
    project = client.project
    print("Datasets in project {}:".format(project))
    for dataset in datasets:
        print("\t{}".format(dataset.dataset_id))  # Print each dataset ID

# Retrieves the secret API key stored in Google Secret Manager
def getSecret():
    client = sm.SecretManagerServiceClient()
    name = f"projects/footballdataproject-458811/secrets/Sport-Data-Keys/versions/1"  # Secret resource name
    response = client.access_secret_version(request={"name": name})  # Access the secret
    return response.payload.data.decode("UTF-8")  # Decode and return the secret value
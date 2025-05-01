# GCP Options

Options related to Google Cloud not dependent on Cloud Dataflow.

## GCP options

reference [docs](https://cloud.google.com/dataflow/docs/reference/pipeline-options#security_and_networking)

| parameter                    | type           | description                                                                                                                                                                                                                                                                                                                                                                       |
|------------------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| project                      | String         | The project ID for your Google Cloud project. The project is required if you want to run your pipeline using the Dataflow managed service.                                                                                                                                                                                                                                        |
| impersonateServiceAccount    | String         | If set, all API requests are made as the designated service account or as the target service account in an impersonation delegation chain. Specify either a single service account as the impersonator, or a comma-separated list of service accounts to create an impersonation delegation chain. This option is only used to submit Dataflow jobs.                              |
| workerRegion                 | String         | Specifies a Compute Engine region for launching worker instances to run your pipeline. This option is used to run workers in a different location than the region used to deploy, manage, and monitor jobs. The zone for workerRegion is automatically assigned. If not set, defaults to the value set for region. (Note: This option cannot be combined with workerZone or zone) |
| workerZone                   | String         | Specifies a Compute Engine zone for launching worker instances to run your pipeline. This option is used to run workers in a different location than the region used to deploy, manage, and monitor jobs. (Note: This option cannot be combined with workerRegion or zone)                                                                                                        |
| dataflowKmsKey               | String         | Specifies the usage and the name of a customer-managed encryption key (CMEK) used to encrypt data at rest. You can control the encryption key through Cloud KMS. You must also specify tempLocation to use this feature.                                                                                                                                                          |
| gcpOauthScopes               | Array<String\> | The OAuth scopes that will be requested when creating the default Google Cloud credentials.                                                                                                                                                                                                                                                                                       |
| pubsub                       | PubSub         | Common Cloud Pub/Sub settings (including emulator settings)                                                                                                                                                                                                                                                                                                                       |
| bigquery                     | BigQuery       | Common BigQuery settings (including emulator settings)                                                                                                                                                                                                                                                                                                                            |
| firestore                    | Firestore      | Common Cloud Firestore settings (including emulator settings)                                                                                                                                                                                                                                                                                                                     |

## PubSub options

reference [docs](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/pubsub/PubsubOptions.html)

| parameter     | type   | description                                                                                                                                                           |
|---------------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| pubsubRootUrl | String | Root URL for use with the Google Cloud Pub/Sub API. In order to use local emulator for Pub/Sub you should use this option to set host and port of your local emulator |

## BigQuery options

reference [docs](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/bigquery/BigQueryOptions.html)

| parameter              | type    | description                                                   |
|------------------------|---------|---------------------------------------------------------------|
| bigqueryProject        | String  | The BigQuery project ID to connect to                         |
| bigqueryEndpoint       | String  | BQ endpoint to use. If unspecified, uses the default endpoint |
| enableStorageReadApiV2 | Boolean | enable storage read api v2. the default is false              |

## Firestore options

reference [docs](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/gcp/firestore/FirestoreOptions.html)

| parameter        | type   | description                                                                                    |
|------------------|--------|------------------------------------------------------------------------------------------------|
| firestoreProject | String | The Firestore project ID to connect to                                                         |
| firestoreHost    | String | A host port pair to allow connecting to a Cloud Firestore instead of the default live service  |
| emulatorHost     | String | A host port pair to allow connecting to a Cloud Firestore emulator instead of the live service |
| firestoreDb      | String | The Firestore database ID to connect to                                                        |

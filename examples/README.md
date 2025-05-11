# Example config files

Here is a list of configuration file examples for common data processing cases.

Try to find and arrange a configuration file that is similar to the data processing you want to do.

* Batch processing
  * Data Transfer
    * [BigQuery to Cloud Spanner](bigquery-to-spanner.json)
    * [BigQuery to Cloud Storage(Parquet)](bigquery-to-parquet.json)
    * [BigQuery to Cloud Firestore](bigquery-to-firestore.json)
    * [BigQuery to Cloud Datastore](bigquery-to-datastore.json)
    * [BigQuery to Cloud Bigtable](bigquery-to-bigtable.json)
    * [BigQuery to AlloyDB](bigquery-to-alloydb.yaml)
    * [BigQuery to Cloud SQL](bigquery-to-jdbc.json)
    * [BigQuery to AWS S3(Avro)](bigquery-to-aws-avro.json)
    * [BigQuery to Vertex AI Matching Engine](bigquery-to-matchingengine.json)
    * [Cloud Spanner to BigQuery](spanner-to-bigquery.json)
    * [Cloud Spanner to Cloud Storage(Avro)](spanner-to-avro.json)
    * [Cloud Spanner to Cloud Datastore](spanner-to-datastore.json)
    * [Cloud Spanner to Cloud SQL](spanner-to-jdbc.json)
    * [Cloud Spanner to Cloud Spanner(Insert)](spanner-to-spanner.json)
    * [Cloud Spanner to Cloud Spanner(Delete)](spanner-to-spanner-delete.json)
    * [Cloud Storage(CSV) to Cloud Spanner](csv-to-spanner.json)
    * [Cloud Storage(Avro) to Cloud Spanner](avro-to-spanner.json)
    * [Cloud Storage(Avro) to Cloud Datastore](avro-to-datastore.json)
    * [Cloud Firestore to BigQuery](firestore-to-bigquery.json)
    * [Cloud Datastore to Cloud Storage(Avro)](datastore-to-avro.json)
    * [Cloud Datastore to Delete](datastore-to-delete.json)
    * [Cloud Bigtable to Delete](bigtable-to-delete.json)
    * [AWS S3(Avro) to Cloud Spanner](aws-avro-to-spanner.json)
    * [Cloud SQL to BigQuery](jdbc-to-bigquery.json)
    * [Google Drive to Cloud Storage](drivefile-to-copyfile.json)
    * [Cloud Storage(Spanner Backup) to Spanner](import-spanner-backup.json)
  * Data Processing
    * [Batch Aggregation (BigQuery)](bigquery-to-aggregation-to-bigquery.json)
    * [BeamSQL: Join BigQuery and Spanner table](beamsql-join-bigquery-and-spanner-to-spanner.json)
    * [Onnx batch inference](bigquery-to-onnx-to-vectorsearch.json)
    * [AutoML(Vertex AI endpoints) batch prediction](bigquery-to-automl-to-spanner.json)
    * [Solr index from PDF files](bigquery-pdf-to-localsolr.json)
    * [Http request for Cloud Run](create-to-http-to-bigquery.json)
    * [Tokenize](bigquery-to-tokenize-to-bigquery.json)
    * [Protobuf deserialize](spanner-to-protobuf-to-avro.json)
    * [Decrypt secret data](spanner-to-decrypt-to-avro.json)
    * [SetOperation: Replace Spanner Table](setoperation-replace-spanner.json)
  * Data Flow Control
    * [Filter](avro-to-filter-to-avro.json)
    * [Partition](avro-to-partition-to-spanner.json)
* Streaming processing
  * Data Transfer
    * [Cloud PubSub(Avro) to BigQuery](pubsub-avro-to-bigquery.json)
    * [Cloud PubSub(Avro) to Cloud Spanner](pubsub-avro-to-spanner.json)
    * [Cloud PubSub(Json) to Vertex AI Matching Engine](pubsub-to-matchingengine.json)
  * Data Processing
    * [Streaming Aggregation (PubSub)](pubsub-to-aggregation-to-pubsub.json)
    * [Cloud PubSub(Json) to BeamSQL to Cloud PubSub(Json)](pubsub-to-beamsql-to-pubsub.json)
  * Microbatch
    * [Spanner(Microbatch) to BigQuery](spanner-microbatch-to-bigquery.json)
    * [BigQuery(Microbatch) to Spanner](bigquery-microbatch-to-spanner.json)
  * Data Flow Control
    * [Union PubSub messages to BigQuery](pubsub-to-union-to-bigquery.json)
  * Dummy data generate
    * [Dummy to PubSub](dummy-to-pubsub.json)
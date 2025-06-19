# Build-in modules

## Source Modules

| module                           | batch | streaming | description                                                        |
|----------------------------------|-------|-----------|--------------------------------------------------------------------|
| [create](source/create.md)       | ○     | ○         | Generate data with specified conditions                            |
| [storage](source/storage.md)     | ○     | ○(TODO)   | Import data from storage on specified path                         |
| [files](source/files.md)         | ○     | ○(TODO)   | Import files from storage on specified path                        |
| [bigquery](source/bigquery.md)   | ○     | ○         | Import data from BigQuery with a specified query or table          |
| [spanner](source/spanner.md)     | ○     | ○         | Import data from Cloud Spanner with a specified query or table     |
| [jdbc](source/jdbc.md)           | ○     | ○(TODO)   | Import data from RDB using JDBC connector with a specified query   |
| [firestore](source/firestore.md) | ○     | ○(TODO)   | Import data from Cloud Firestore with a specified filter condition |
| [datastore](source/datastore.md) | ○     | ○(TODO)   | Import data from Cloud Datastore with a specified gql              |
| [bigtable](source/bigtable.md)   | ○     | ○(TODO)   | Import data from Cloud Bigtable with a specified condition         |
| [pubsub](source/pubsub.md)       | -     | ○         | Import data from Cloud PubSub                                      |
| [drivefile](source/drivefile.md) | ○     | -         | Import file info from Google Drive                                 |
| [websocket](source/websocket.md) | -     | ○         | Import data from WebSocket                                         |

## Transform Modules

| module                                  | batch | streaming | description                                                                           |
|-----------------------------------------|-------|-----------|---------------------------------------------------------------------------------------|
| [beamsql](transform/beamsql.md)         | ○     | ○         | Process the data in a given SQL (by Beam SQL)                                         |
| [select](transform/select.md)           | ○     | ○         | Filter and process input rows according to specified conditions                       |
| [partition](transform/partition.md)     | ○     | ○         | Splits a data collection into separate data collections based on specified conditions |
| [aggregation](transform/aggregation.md) | ○     | ○         | Performs aggregation from a simple aggregation process definition.                    |
| [http](transform/http.md)               | ○     | ○         | Send http request to the specified endpoint and get the result                        |
| [onnx](transform/onnx.md)               | ○     | ○         | Make inferences using the specified [onnx](https://onnxruntime.ai/) file              |
| [deserialize](transform/deserialize.md) | ○     | ○         | Deserialize a value serialized in specified format.                                   |
| [tokenize](transform/tokenize.md)       | ○     | ○         | Tokenizes and processes input text                                                    |
| [pdfextract](transform/pdfextract.md)   | ○     | ○         | Extract text and metadata from PDF files                                              |
| [reshuffle](transform/reshuffle.md)     | ○     | ○         | Insert reshuffle stage to prevent dataflow fusion optimizations                       |

## Sink Modules

| module                                   | batch | streaming | description                                                |
|------------------------------------------|-------|-----------|------------------------------------------------------------|
| [storage](sink/storage.md)               | ○     | ○(TODO)   | Write file to Cloud Storage                                |
| [bigquery](sink/bigquery.md)             | ○     | ○         | Inserting Data into BigQuery Table                         |
| [spanner](sink/spanner.md)               | ○     | ○         | Inserting Data into Cloud Spanner Table                    |
| [jdbc](sink/jdbc.md)                     | ○     | ○(TODO)   | Inserting Data into RDB table using JDBC connector         |
| [firestore](sink/firestore.md)           | ○     | ○         | Inserting Data into Cloud Firestore Collection             |
| [datastore](sink/datastore.md)           | ○     | ○         | Inserting Data into Cloud Datastore kind                   |
| [bigtable](sink/bigtable.md)             | ○     | ○         | Inserting Data into Cloud Bigtable table                   |
| [pubsub](sink/pubsub.md)                 | ○     | ○         | Publish data to specified PubSub topic                     |
| [copyfile](sink/copyfile.md)             | ○     | ○(TODO)   | Copy files between storage services                        |
| [debug](sink/debug.md)                   | ○     | ○         | Outputting data to the log                                 |

## Failure Modules

| module                      | batch | streaming | description                                                      |
|-----------------------------|-------|-----------|------------------------------------------------------------------|
| [pubsub](failure/pubsub.md) | ○     | ○         | Publish Failure record messages to specified Cloud Pub/Sub topic |

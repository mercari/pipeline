# Storage Source Module

Source Module for loading data by specifying a path into Cloud Storage or S3(AWS).

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                           |
|--------------------|----------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                     |
| module             | required | String              | Specified `storage`                                                                                                                   |
| schema             | optional | [Schema](SCHEMA.md) | Schema of the data to be read. No need to specify if avro or parquet format                                                           |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp or Date type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                           |

## Storage source module parameters

| parameter      | optional | type                    | description                                                                                                                                          |
|----------------|----------|-------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| input (inputs) | required | String (Array<String\>) | GCS or S3 path. Always start with `gs://` or `s3://`                                                                                                 |
| format         | required | Enum                    | The data format of the file to be read. Corresponding to `csv`, `json`, `avro`, and `parquet`. (JSON is comma-separated json)                        |
| compression    | optional | Enum                    | (Only if you specify CSV or JSON in the format)Compression format of the file to be read. Supports `zip`, `gzip`, `bzip2`, `zstd`, `lzo`, and `lzop` |
| filterPrefix   | optional | String                  | (Only if you specify CSV or JSON in the format)Specify the Prefix of the line you want to skip, such as the CSV Header.                              |

## Related example config files

* [Cloud Storage(Avro) to Cloud Spanner](../../../../examples/avro-to-spanner.json)
* [Cloud Storage(Avro) to Cloud Datastore](../../../../examples/avro-to-datastore.json)
* [AWS S3(Avro) to Cloud Spanner](../../../../examples/aws-avro-to-spanner.json)

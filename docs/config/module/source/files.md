# Files Source Module

Source Module for loading data by specifying a path into Cloud Storage or S3(AWS).

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                           |
|--------------------|----------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                     |
| module             | required | String              | Specified `files`                                                                                                                     |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp or Date type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                           |

## Files source module parameters

| parameter           | optional | type                    | description                                                 |
|---------------------|----------|-------------------------|-------------------------------------------------------------|
| pattern (patterns)  | required | String (Array<String\>) | GCS or S3 path. Always start with `gs://` or `s3://`        |
| withContent         | optional | Boolean                 | specify requires file content or not (the default is false) |
| emptyMatchTreatment | optional | Enum                    | specify `ALLOW`, `DISALLOW` or `ALLOW_IF_WILDCARD`          |

## Output Schema

The results of tokenization are stored as an array with the following schema as values, for each field specified by the parameter `fields`.

| field               | type      | description                                      |
|---------------------|-----------|--------------------------------------------------|
| filename            | String    | File name                                        |
| directory           | String    | Name of the directory where this file is located |
| resource            | String    | Full resource name                               |
| sizeBytes           | Integer   | File byte size                                   |
| isDirectory         | Boolean   | Whether the file is a directory                  |
| lastModified        | Timestamp | Timestamp the file was last modified             |
| schema              | String    | schema (such as `gs`, `s3`)                      |
| isReadSeekEfficient | Boolean   | Whether the file can be efficiently seek         |
| checksum            | String    | checksum                                         |
| content             | Bytes     | File content                                     |
| compression         | String    | File compression method                          |


## Related example config files

* [Files on Cloud Storage to aggregate Cloud Storage](../../../../examples/files-to-storage.json)

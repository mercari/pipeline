# Bigtable Sink Module (Experimental)

Sink module to write(or delete) inputs data to a specified Cloud Bigtable table.

## Sink module common parameters

| parameter  | optional | type                                    | description                                                               |
|------------|----------|-----------------------------------------|---------------------------------------------------------------------------|
| name       | required | String                                  | Step name. specified to be unique in config file.                         |
| module     | required | String                                  | Specified `bigtable`                                                      |
| inputs     | required | Array<String\>                          | Step names whose data you want to write from                              |
| parameters | required | Map<String,Object\>                     | Specify the following individual parameters.                              |
| logging    | optional | Array<[Logging](../common/logging.md)\> | Step names whose data you want to print log. support `input` and `output` |

## Bigtable sink module parameters

| parameter              | optional | type                          | description                                                                                                                                                                                                                            |
|------------------------|----------|-------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId              | required | String                        | Cloud Bigtable's GCP project ID that you want to write                                                                                                                                                                                 |
| instanceId             | required | String                        | The instance ID of the Cloud Bigtable you want to write                                                                                                                                                                                |
| tableId                | required | String                        | The table name of the Cloud Bigtable you want to write                                                                                                                                                                                 |
| rowKey                 | required | String                        | Specify the template text when you want to specify the rowKey value by conversion using template engine [FreeMarker](https://freemarker.apache.org/)                                                                                   |
| columns                | required | Array<Column\>                | Specify column insertion settings. (If you specify `DELETE_FROM_ROW` in `mutationOp`, it is not required)                                                                                                                              |
| format                 | optional | [Enum](../common/bigtable.md) | Specify the cell value serialization format.　One of `bytes`, `avro` or `text`. Used as default value if not specified in each `columns` parameter. The default is `bytes`.                                                             |
| mutationOp             | optional | Enum                          | Specify the change type you want to make to the row. One of `SET_CELL`, `DELETE_FROM_COLUMN`, `DELETE_FROM_FAMILY` or `DELETE_FROM_ROW`. Used as default value if not specified in each `columns` parameter. The default is `SET_CELL` |
| timestampType          | optional | Enum                          | Specify the type of timestamp to apply cells. One of `server`,`event`,`field`,`fixed`. Used as default value if not specified in each `columns` parameter. The default is `server`                                                     |
| appProfileId           | optional | String                        | Specify the app profile id                                                                                                                                                                                                             |
| flowControl            | optional | Boolean                       | Specify flow control enabled                                                                                                                                                                                                           |
| maxBytesPerBatch       | optional | Integer                       | Specify max bytes a batch can have                                                                                                                                                                                                     |
| maxElementsPerBatch    | optional | Integer                       | Specify max elements a batch can have                                                                                                                                                                                                  |
| maxOutstandingBytes    | optional | Integer                       | Specify max number of outstanding bytes allowed before enforcing flow control                                                                                                                                                          |
| maxOutstandingElements | optional | Integer                       | Specify max number of outstanding elements allowed before enforcing flow control                                                                                                                                                       |
| batching               | optional | Boolean                       | Enabling this function reduces the IO load on Bigtable by aggregating mutations by key. Consider using this function when the mutation contains a large number of columns. (The default is false)                                      |

## Column parameters

Specify the writing cell settings for each column family.
If the parameters `format`,`mutationOp`,`timestampType` are not specified, the upper-level setting is applied as default.

| parameter     | optional | type              | description                                                                                                                                                        |
|---------------|----------|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| family        | required | String            | Specify the columnFamily name to be assigned to qualifiers                                                                                                         |
| qualifiers    | required | Array<Qualifier\> | Specify the columnQualifiers settings to be assigned to the columnFamily. (If you specify `DELETE_FROM_FAMILY` in `mutationOp`, it is not required)                |
| format        | optional | Enum              | Specify the cell value serialization format. The default is parent `format` value                                                                                  |
| mutationOp    | optional | Enum              | Specify the row change type to be assigned to the field. One of `SET_CELL`, `DELETE_FROM_COLUMN` or `DELETE_FROM_FAMILY`. The default is parent `mutationOp` value |
| timestampType | optional | Enum              | Specify the time to use as the timestamp for cell. The default is parent `timestampType` value                                                                     |

## Qualifier parameters

Specify the settings for each column qualifier.
If the parameters `format`,`mutationOp`,`timestampType` are not specified, the upper-level setting is applied as default.

| parameter     | optional | type   | description                                                                                                                                  |
|---------------|----------|--------|----------------------------------------------------------------------------------------------------------------------------------------------|
| name          | optional | String | Specify columnQualifier name to be assigned to the field                                                                                     |
| field         | optional | String | Specify field name to insert a value into the cell                                                                                           |
| format        | optional | Enum   | Specify the cell value serialization format. The default is parent `format` value                                                            |
| mutationOp    | optional | Enum   | Specify the row change type to be assigned to the field. One of `SET_CELL` or `DELETE_FROM_COLUMN`. The default is parent `mutationOp` value |
| timestampType | optional | Enum   | Specify the time to use as the timestamp for cell. The default is parent `timestampType` value                                               |

## MutationOp

Specifies a particular change to be made to the target cell or row

| mutationOp         | description                                                      |
|--------------------|------------------------------------------------------------------|
| SET_CELL           | Mutation to set value to cell                                    |
| ADD_TO_CELL        | Mutation to add value to cell (for only aggregation cell)        |
| DELETE_FROM_COLUMN | Mutation to delete all cells from the specified column qualifier |
| DELETE_FROM_FAMILY | Mutation to delete all cells from the specified column family    |
| DELETE_FROM_ROW    | Mutation to delete all cells from the specified row key          |

## TimestampType

Contents of the timestamp value to be set in the cell

| mutationOp | description                                                             |
|------------|-------------------------------------------------------------------------|
| server     | Server timestamp at the time it was sent to Bigtable                    |
| event      | Event times for data events assigned by Apache Beam                     |
| field      | Use the value of the field specified in `timestampField` as a timestamp |
| fixed      | Use the value specified in `timestampValue` as a fixed value timestamp  |
| zero       | Set timestamp as unspecified (set as epochMicros=0)                     |

## Example

* Example of deleting all cells of the same column and then insert

Write is atomic on a per-row basis, and mutations are applied in the order defined.
In the following definition, the data in the specified column is deleted first, and then the data is inserted.
This is used when you only want to put one value in one cell.

```json
{
  "sources": [
    {
      "name": "BigQueryInput",
      "module": "bigquery",
      "parameters": {
        "table": "example-project.exampledataset.user_activity",
        "fields": ["user_id","event_name","created_at","action_type","item_id"]
      }
    }
  ],
  "sinks": [
    {
      "name": "BigtableOutput",
      "module": "bigtable",
      "inputs": ["BigQueryInput"],
      "parameters": {
        "projectId": "example-project",
        "instanceId": "example-instance",
        "tableId": "example-table",
        "rowKey": "${user_id}#${event_name}#${utils.bigtable.reverseTimestampMicros(created_at)}",
        "timestampType": "server",
        "columns": [
          {
            "family": "a",
            "qualifiers": [
              { "name": "iid", "field": "item_id" },
              { "name": "at", "field": "action_type" }
            ],
            "mutationOp": "DELETE_FROM_COLUMN"
          },
          {
            "family": "a",
            "qualifiers": [
              { "name": "iid", "field": "item_id" },
              { "name": "at", "field": "action_type" }
            ],
            "mutationOp": "SET_CELL"
          }
        ]
      }
    }
  ]
}
```

### Signatures of build-in utility functions for template engine

There are built-in functions for date and timestamp formatting available in the `rowKey`, `columns[].family`, `columns[].qualifiers[].name`.

```
// Text format function for date field
${utils.datetime.formatDate(dateField, 'yyyyMMdd')}

// Text format function for timestamp field
${utils.datetime.formatTimestamp(timestampField, 'yyyyMMddhhmmss', 'Asia/Tokyo')}

// The event timestamp implicitly assigned to a record can be referenced by `context.timestamp`.
${utils.datetime.formatTimestamp(context.timestamp, 'yyyyMMddhhmmss', 'Asia/Tokyo')}


// rowKey template example
${user_id}#${utils.bigtable.reverseTimestampMicros(timestampField)}
```

## Related example config files

* [BigQuery to Cloud Bigtable](../../../../examples/bigquery-to-bigtable.json)
* [Cloud Bigtable to delete](../../../../examples/bigtable-to-delete.json)

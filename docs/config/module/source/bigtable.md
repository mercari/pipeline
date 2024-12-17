# Bigtable Source Module (Experimental)

Source Module for loading data by specifying filter conditions from Cloud Bigtable.

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                                                                   |
|--------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                                                                             |
| module             | required | String              | Specified `bigtable`                                                                                                                                                          |
| schema             | -        | [Schema](SCHEMA.md) | Schema of the data to be read. bigtable module does not require specification                                                                                                 |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp or Date type)                                         |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters.                                                                                                                                  |

## Bigtable source module parameters

| parameter    | optional           | type                              | description                                                                                                                                                                                                                                                                                                  |
|--------------|--------------------|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId    | required           | String                            | Cloud Bigtable's GCP project ID that you want to read                                                                                                                                                                                                                                                        |
| instanceId   | required           | String                            | The instance ID of the Cloud Bigtable you want to read                                                                                                                                                                                                                                                       |
| tableId      | required           | String                            | The table name of the Cloud Bigtable you want to read                                                                                                                                                                                                                                                        |
| keyRange     | selective required | [KeyRange](../common/bigtable.md) | Specify key range conditions for queries from request to Bigtable                                                                                                                                                                                                                                            |
| filter       | selective required | [Filter](../common/bigtable.md)   | Specify filter conditions for query from request to Bigtable                                                                                                                                                                                                                                                 |
| columns      | required           | Array<Column\>                    | Define the output schema by defining the mapping of each column to its type and feed name                                                                                                                                                                                                                    |
| format       | optional           | [Enum](../common/bigtable.md)     | Specify the cell value serialization format.　One of `bytes`, `avro` or `text`. Used as default value if not specified in each `columns` parameter. The default is `bytes`.                                                                                                                                   |
| cellType     | optional           | Enum                              | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. Used as default value if not specified in each `columns` parameter. One of `last`, `first` or `all`. The default is `first`. If `all` is specified, it will be an array of the type specified in `type` |
| appProfileId | optional           | String                            | Specify the app profile id                                                                                                                                                                                                                                                                                   |

### Column parameters

Specify the writing cell settings for each column family.
If the parameters `format`,`cellType` are not specified, the upper-level setting is applied as default.

| parameter  | optional | type              | description                                                                                                                                 |
|------------|----------|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| family     | required | String            | Specify the columnFamily name to be assigned to qualifiers                                                                                  |
| qualifiers | required | Array<Qualifier\> | Specify the columnQualifiers settings to be assigned to the columnFamily.                                                                   |
| format     | optional | Enum              | Specify the cell value serialization format. The default is parent `format` value                                                           |
| cellType   | optional | Enum              | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. The default is parent `cellType` value |

### Qualifier parameters

Specify the settings for each column qualifier.
If the parameters `format`,`cellType` are not specified, the upper-level setting is applied as default.

| parameter | optional | type   | description                                                                                                                                 |
|-----------|----------|--------|---------------------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String | Specify columnQualifier name to be assigned to the field                                                                                    |
| field     | required | String | Specify the name of the field that holds the retrieved column qualifier cell value                                                          |
| type      | required | String | Specify the field type that holds the retrieved column qualifier cell value                                                                 |
| format    | optional | Enum   | Specify the cell value serialization format. The default is parent `format` value                                                           |
| cellType  | optional | Enum   | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. The default is parent `cellType` value |


## Example

```json
{
  "sources": [
    {
      "name": "BigtableInput",
      "module": "bigtable",
      "parameters": {
        "projectId": "example-project",
        "instanceId": "example-instance",
        "tableId": "example-table",
        "rowFilter": {
          "type": "timestamp_range",
          "start": "2024-01-01T00:00:00Z",
          "end": "2024-12-31T23:59:59Z"
        },
        "columns": [
          {
            "family": "a",
            "qualifiers": [
              { "name": "iid", "field": "item_id", "type": "string" },
              { "name": "at", "field": "action_type", "type": "string" },
              { "name": "am", "field": "amount", "type": "int64" }
            ],
            "cellType": "last"
          },
          {
            "family": "b",
            "qualifiers": [
              { "name": "in", "field": "item_name" },
              { "name": "iu", "field": "image_url", "cellType": "all" }
            ],
            "cellType": "last"
          }
        ],
        "withRowKey": true,
        "rowKeyField": "row_key"
      }
    }
  ]
}
```

The above configuration retrieves records with the following schema.

```json
{
  "row_key": "1234567890#",
  "item_id": "xxx",
  "action_type": "buy",
  "amount": 3000,
  "item_name": "book",
  "image_url": [
    "https://xxx/yyy/001.png",
    "https://xxx/yyy/002.png",
    "https://xxx/yyy/003.png"
  ]
}
```

## Related example config files

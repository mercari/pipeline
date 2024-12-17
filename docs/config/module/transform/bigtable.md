# Bigtable Transform Module

Bigtable transform module can be used to query rows from bigtable and processing the rows.

## Transform module common parameters

| parameter  | optional | type                | description                                                                                                 |
|------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                                                           |
| module     | required | String              | Specified `bigtable`                                                                                        |
| inputs     | required | Array<String\>      | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                                |

## Filter transform module parameters

| parameter    | optional           | type                                    | description                                                                                                                                                                                                                                                                                                  |
|--------------|--------------------|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId    | required           | String                                  | Cloud Bigtable's GCP project ID that you want to write                                                                                                                                                                                                                                                       |
| instanceId   | required           | String                                  | The instance ID of the Cloud Bigtable you want to write                                                                                                                                                                                                                                                      |
| tableId      | required           | String                                  | The table name of the Cloud Bigtable you want to write                                                                                                                                                                                                                                                       |
| keyRange     | selective required | [KeyRange](../common/bigtable.md)       | Specify key range conditions for filtering rows                                                                                                                                                                                                                                                              |
| filter       | selective required | [Filter](../common/bigtable.md)         | Specify the conditions for filtering rows and cells                                                                                                                                                                                                                                                          |
| columns      | required           | Array<[Column](../source/bigtable.md)\> | Define the output schema by defining the mapping of each column to its type and feed name                                                                                                                                                                                                                    |
| format       | optional           | [Enum](../common/bigtable.md)           | Specify the cell value serialization format.　One of `bytes`, `avro` or `text`. Used as default value if not specified in each `columns` parameter. The default is `bytes`.                                                                                                                                   |
| cellType     | optional           | Enum                                    | Specify cellType that defines how to retrieve the multiple cells associated with a column qualifier. Used as default value if not specified in each `columns` parameter. One of `last`, `first` or `all`. The default is `first`. If `all` is specified, it will be an array of the type specified in `type` |
| sql          | optional           | String                                  | If you want to post-process the data retrieved from bigtable, specify the SQL for that processing.                                                                                                                                                                                                           |
| appProfileId | optional           | String                                  | Specify the app profile id                                                                                                                                                                                                                                                                                   |

## Example

```json
{
  "transforms": [
    {
      "name": "BigtableInput",
      "module": "bigtable",
      "inputs": [
        "pubsubInput"
      ],
      "parameters": {
        "projectId": "example-project",
        "instanceId": "example-instance",
        "tableId": "example-table",
        "keyRange": {
          "prefix": "${user_id}#"
        },
        "filter": {
          "type": "family_name_regex",
          "exact": "e"
        },
        "columns": [
          {
            "family": "e",
            "qualifiers": [
              { "name": "iid", "field": "item_id", "type": "string" },
              { "name": "at", "field": "action_type", "type": "string" },
              { "name": "am", "field": "amount", "type": "int64" }
            ],
            "cellType": "last"
          }
        ],
        "sql": "SELECT SUM(amount) AS 'sum', COUNT(1) AS `count` FROM INPUT"
      }
    }
  ]
}
```
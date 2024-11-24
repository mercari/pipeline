# Bigtable Sink Module (Experimental)

Sink module to write the input data to a specified Cloud Bigtable table.

## Sink module common parameters

| parameter        | optional | type                | description                                                                             |
|------------------|----------|---------------------|-----------------------------------------------------------------------------------------|
| name             | required | String              | Step name. specified to be unique in config file.                                       |
| module           | required | String              | Specified `bigtable`                                                                    |
| inputs           | required | Array<String\>      | Step names whose data you want to write from                                            |
| parameters       | required | Map<String,Object\> | Specify the following individual parameters.                                            |

## Bigtable sink module parameters

| parameter     | optional | type           | description                                                                                                                                                        |
|---------------|----------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| projectId     | required | String         | Cloud Bigtable's GCP Project ID that you want to write                                                                                                             |
| instanceId    | required | String         | The Instance ID of the Cloud Bigtable you want to write                                                                                                            |
| tableId       | required | String         | The table name of the Cloud Bigtable you want to write                                                                                                             |
| rowKey        | required | String         | Specify the template text when you want to specify the rowKey value by conversion using template engine [FreeMarker](https://freemarker.apache.org/)               |
| columns       | required | Array<Column\> | Specify column insertion settings. (If you specify `DELETE_FROM_ROW` in `mutationOp`, it is not required)                                                          |
| format        | optional | Enum           | Specify the serialization format.　One of `bytes`, `avro` or `string`. The default is `bytes`.                                                                      |
| mutationOp    | optional | Enum           | Specify the change type you want to make to the row. One of `SET_CELL`, `DELETE_FROM_COLUMN`, `DELETE_FROM_FAMILY` or `DELETE_FROM_ROW`. The default is `SET_CELL` |
| timestampType | optional | Enum           | Specify the time to use as the timestamp for cell. One of `current_timestamp` or `event_timestamp`. The default is `current_timestamp`                             |

## Column parameters

Specify the write cell config for each column.
If the option is not specified, the value of the whole option will be used as the default.

| parameter     | optional | type              | description                                                                                                                                         |
|---------------|----------|-------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| family        | required | String            | Specify the columnFamily name to be assigned to qualifiers                                                                                          |
| qualifiers    | required | Array<Qualifier\> | Specify the columnQualifiers settings to be assigned to the columnFamily. (If you specify `DELETE_FROM_FAMILY` in `mutationOp`, it is not required) |
| format        | optional | Enum              | Specify the serialization format.　One of `bytes`, `avro` or `string`. The default is `bytes`.                                                       |
| mutationOp    | optional | Enum              | Specify the row change type to be assigned to the field. One of `SET_CELL`, `DELETE_FROM_COLUMN` or `DELETE_FROM_FAMILY`                            |
| timestampType | optional | Enum              | Specify the time to use as the timestamp for cell. One of `current_timestamp` or `event_timestamp`. The default is `current_timestamp`              |

## Qualifier parameters

Specify the settings for each column.
If the option is not specified, the value of the whole option will be used as the default.

| parameter  | optional | type   | description                                                                                        |
|------------|----------|--------|----------------------------------------------------------------------------------------------------|
| name       | optional | String | Specify columnQualifier name to be assigned to the field                                           |
| field      | optional | String | Specify field name to insert a value into the cell                                                 |
| format     | optional | Enum   | Specify the serialization format.　One of `bytes`, `avro` or `string`. The default is `bytes`.      |
| mutationOp | optional | Enum   | Specify the row change type to be assigned to the field. One of `SET_CELL` or `DELETE_FROM_COLUMN` |

## MutationOp

Specifies a particular change to be made to the contents of a row

| mutationOp         | description                                                        |
|--------------------|--------------------------------------------------------------------|
| SET_CELL           | Mutation which sets values of fields contained in a record         |
| DELETE_FROM_COLUMN | Mutation which deletes cells from the fields contained in a record |
| DELETE_FROM_FAMILY | Mutation which deletes all cells from the specified column family  |
| DELETE_FROM_ROW    | Mutation which deletes all cells from the containing row           |

### Signatures of build-in utility functions for template engine

There are built-in functions for date and timestamp formatting available in the `rowKey`, `columns[].family`, `columns[].qualifiers[].name`.

```
// Text format function for date field
${_DateTimeUtil.formatDate(dateField, 'yyyyMMdd')}

// Text format function for timestamp field
${_DateTimeUtil.formatTimestamp(timestampField, 'yyyyMMddhhmmss', 'Asia/Tokyo')}

// The event timestamp implicitly assigned to a record can be referenced by _EVENTTIME.
${_DateTimeUtil.formatTimestamp(_EVENTTIME, 'yyyyMMddhhmmss')}
```

## Related example config files

* [BigQuery to Cloud Bigtable](../../../../examples/bigquery-to-bigtable.json)

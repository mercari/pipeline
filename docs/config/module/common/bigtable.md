# Bigtable module common properties

## Format

Cell data serialization format. The default is `bytes`

| mutationOp | description                                                                                                                                            |
|------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| bytes      | Serialized Values of primitive types in big-endian(Same format as Bytes in HBase). For arrays and structures, they are serialized as JSON strings.     |
| avro       | Serialize in Avro format with the specified schema. This is used when you want to serialize a structure in an efficient way, instead of a JSON string. |
| text       | Serialize all types as strings. Arrays and structures are treated as JSON strings. High readability but poor compression efficiency.                   |

### Serialization format for each data type.

(For more information on Avro's serialization format, please refer to the [official document](https://avro.apache.org/docs/1.11.1/specification/#binary-encoding))

| -         | bytes                  | avro                    | text                   |
|-----------|------------------------|-------------------------|------------------------|
| boolean   | bit                    | bit                     | as string              |
| string    | utf-8                  | long(size) + utf-8      | utf-8                  |
| bytes     | byte array             | long(size) + byte array | byte array             |
| int32     | big-endian             | variable-length zig-zag | as string              |
| int64     | big-endian             | variable-length zig-zag | as string              |
| float32   | big-endian             | little-endian           | as string              |
| float64   | big-endian             | little-endian           | as string              |
| date      | as int32(epoch days)   | as int32(epoch days)    | as string              |
| time      | as int64(epoch micros) | as int64(epoch micros)  | as string              |
| timestamp | as int64(epoch micros) | as int64(epoch micros)  | as string              |
| enum      | as int32(index)        | as int32(index)         | as string              |
| array     | as string(json array)  | avro array              | as string(json array)  |
| struct    | as string(json object) | avro record             | as string(json object) |
| map       | as string(json object) | avro map                | as string(json object) |
| null      | empty byte array       | empty byte array        | empty byte array       |

## KeyRange

| parameter | description                                      |
|-----------|--------------------------------------------------|
| start     | Start value of rowKey scan range                 |
| end       | End value of rowKey scan range                   |
| prefix    | Specify rowKey scan range by prefix              |
| exact     | Specify exact rowKey (only for transform module) |

## Filters

Conditions for filtering rows.
If specified as an array, the filters are executed in order as a chain filter.
See the [official documentation](https://cloud.google.com/bigtable/docs/filters) for details.

| category       | type                      | parameters                                       | description                                                                                                   |
|----------------|---------------------------|--------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| limiting(row)  | sample                    | rate                                             | Retrieve a random sample of rows                                                                              |
| limiting(row)  | row_key_regex             | regex,exact                                      | Include only cells whose row key matches a regular expression                                                 |
| limiting(cell) | limit_cells_per_row       | limit                                            | Include only the first N cells from a row                                                                     |
| limiting(cell) | limit_cells_per_column    | limit                                            | Include only the N most recent versions of a column in a row                                                  |
| limiting(cell) | offset_cells_per_row      | offset                                           | Omit the first N cells from a row.                                                                            |
| limiting(cell) | family_name_regex         | regex,exact                                      | Include only cells whose column family matches an RE2 regular expression                                      |
| limiting(cell) | column_qualifier_regex    | regex,exact                                      | Include only cells whose column qualifier matches a regular expression                                        |
| limiting(cell) | column_range              | family,start(Open or Closed),end(Open or Closed) | Include only cells in a specific column family whose column qualifier is within a specific range              |
| limiting(cell) | value_range               | start(Open or Closed),end(Open or Closed)        | Include only cells whose value falls within a specific range                                                  |
| limiting(cell) | value_regex               | regex,exact                                      | Include only cells whose value matches a regular expression                                                   |
| limiting(cell) | timestamp_range           | start,end                                        | Include only cells whose timestamp falls within a specific range                                              |
| limiting       | block                     |                                                  | Don't emit any cells. Mostly useful for debugging                                                             |
| limiting       | pass                      |                                                  | Emit all input cells. Mostly useful for debugging                                                             |
| limiting       | sink                      |                                                  | Include cells in the final output row, and prevent them from being modified or removed by a subsequent filter |
| modifying      | label                     | label                                            | Add a label to all cells                                                                                      |
| modifying      | strip                     |                                                  | Return an empty string for each cell value                                                                    |
| composing      | chain                     | children                                         | Apply multiple filters in order (Like `AND`)                                                                  |
| composing      | interleave                | children                                         | Combine output rows from multiple filters into a single output row (Like `OR`)                                |
| composing      | condition(Not supported)  |                                                  | Apply one of two possible filters to a row                                                                    |

### Filters example

```json
{
  "name": "bigtableSource",
  "module": "bigtable",
  "parameters": {
    "projectId": "myproject",
    "instanceId": "myinstance",
    "tableId": "mytable",
    "keyRange": {
      "start": "${user_id}#1",
      "end": "${user_id}#5"
    },
    "filter": [
      { "type": "sample", "rate": 0.001 },
      { "type": "row_key_regex", "regex": "${user_id}#*" },
      { "type": "limit_cells_per_row", "limit": 1 },
      { "type": "limit_cells_per_column", "limit": 1 },
      { "type": "offset_cells_per_row", "offset": 1 },
      { "type": "family_name_regex", "regex": "fn" },
      { "type": "column_qualifier_regex", "regex": "qf" },
      { "type": "column_range", "startClosed": "qf", "endOpen": "", "family": "fn" },
      { "type": "value_range", "startClosed": "qf", "endClosed": "" },
      { "type": "value_regex", "regex": "qf" },
      { "type": "timestamp_range", "start": "2024-01-01T00:00:00Z", "end": "2025-01-01T00:00:00Z" },
      { "type": "label", "label": "done" },
      { "type": "strip" },
      { "type": "chain", "children": [
        { "type": "family_name_regex", "regex": "fn" },
        { "type": "sample", "rate": 0.001 }
      ] },
      { "type": "interleave", "children": [
        { "type": "family_name_regex", "regex": "fn" },
        { "type": "sample", "rate": 0.001 }
      ] },
      { "type": "block" },
      { "type": "pass" },
      { "type": "sink" }
    ],
    "columns": []
  }
}
```







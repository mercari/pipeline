# Avro Schema

## Schema List

| name           | description                                                                      | file                                       |
|----------------|----------------------------------------------------------------------------------|--------------------------------------------|
| BadRecord      | Schema to hold error information. Used for storing in DWH, Messaging Queue, etc. | [badrecord.avsc](badrecord.avsc)           |
| Bigtable Cells | Schema of Cloud Bigtable Cells.                                                  | [bigtable_cells.avsc](bigtable_cells.avsc) |
| Bigtable CDC   | Schema of Cloud Bigtable Change Stream Record.                                   | [bigtable_cdc.avsc](bigtable_cdc.avsc)     |


## BigQuery compatible table schema

### BadRecord

```json
[
  {
    "name": "job",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "module",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "record",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "coder",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "json",
        "type": "JSON",
        "mode": "NULLABLE"
      },
      {
        "name": "bytes",
        "type": "BYTES",
        "mode": "NULLABLE"
      }
    ]
  },
  {
    "name": "failure",
    "type": "RECORD",
    "mode": "NULLABLE",
    "fields": [
      {
        "name": "description",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "exception",
        "type": "STRING",
        "mode": "NULLABLE"
      },
      {
        "name": "stacktrace",
        "type": "STRING",
        "mode": "NULLABLE"
      }
    ]
  },
  {
    "name": "timestamp",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  },
  {
    "name": "eventtime",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  }
]
```

### PubsubMessage

```json
[
  {
    "name": "subscription_name",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": ""
  },
  {
    "name": "message_id",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": ""
  },
  {
    "name": "publish_time",
    "mode": "NULLABLE",
    "type": "TIMESTAMP",
    "description": ""
  },
  {
    "name": "data",
    "mode": "NULLABLE",
    "type": "BYTES",
    "description": ""
  },
  {
    "name": "attributes",
    "mode": "NULLABLE",
    "type": "JSON",
    "description": ""
  }
]
```

# Auxia Sink Module

Sink module for send event stream to [Auxia](https://www.auxia.io/).
Event data can be synchronized simply, without the need to implement conversions using protobufs.

## Sink module common parameters

| parameter  | optional | type                                    | description                                       |
|------------|----------|-----------------------------------------|---------------------------------------------------|
| name       | required | String                                  | Step name. specified to be unique in config file. |
| module     | required | String                                  | Specified `auxia`                                 |
| inputs     | required | Array<String\>                          | Source step names you want to integrate to Auxia  |
| parameters | required | Map<String,Object\>                     | Specify the following individual parameters.      |
| loggings   | optional | Array<[Logging](../common/logging.md)\> | Logging settings (support `input`)                |

## Auxia sink module parameters

| parameter     | optional | type                | description                                                                                            |
|---------------|----------|---------------------|--------------------------------------------------------------------------------------------------------|
| projectId     | required | String              | Specify the projectId of the Auxia you wish to link                                                    |
| type          | optional | Enum                | Choose `element` or `json`. If you want to parse JSON type data, choose `json`. Default is `element`.  |
| field         | optional | String              | When parsing JSON, specify the field name with JSON value.                                             |
| eventName     | optional | String              | If the data flows to this sink all have the same event_name, it can be specified here as a fixed value |
| pubsub        | optional | Map<String,Object\> | Specify the resource name of the topic of the pubsub you wish to link to Auxia                         |
| excludeFields | optional | Array<String\>      |                                                                                                        |


## Input schema fields

The input record must contain the fields `required` in the following reserved fields list().
Fields other than the names listed below will be linked as event properties

reference: https://docs.auxia.io/data-ingestion/overview/user-event-data

| field name                | optional           | type      | description                                                                                                                                                                                                                  |
|---------------------------|--------------------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| user_id                   | required           | String    | Unique identifier for every user                                                                                                                                                                                             |
| event_name                | selective required | String    | Unique identifier for the event. (If `eventName` was specified as a parameter, it is not needed)                                                                                                                             |
| client_event_timestamp    | required           | Timestamp | The timestamp of the event                                                                                                                                                                                                   |
| server_received_timestamp | optional           | Timestamp | The timestamp at which the event was received by the server                                                                                                                                                                  |
| insert_id                 | optional           | String    | Unique identifier for the user that can be joined to events and attributes                                                                                                                                                   |
| pre_login_temp_user_id    | optional           | String    | Unique identifier for the user when the user was signed out. This identifier is used to identify a user across signed out and signed in sessions. Required to support merging user across signed out and signed in sessions. |
| session_id                | optional           | String    | Unique identifier for a single user session                                                                                                                                                                                  |
| country                   | optional           | String    | Country of the user                                                                                                                                                                                                          |
| region                    | optional           | String    | Region of the user                                                                                                                                                                                                           |
| city                      | optional           | String    | City of the user                                                                                                                                                                                                             |
| ip_address                | optional           | String    | IP Address of the user                                                                                                                                                                                                       |
| device_id                 | optional           | String    | Unique identifier for the user's device                                                                                                                                                                                      |
| app_version_id            | optional           | String    | Version of the app that the events correspond to                                                                                                                                                                             |

## Example

#### flat schema

For flat schemas, fields other than the reserved fields above are automatically converted to event_properties

```yaml
options:
  streaming: true
sources:
  name: input
  module: create
  parameters:
    elements:
      - user_id: "u000001"
        client_event_timestamp: "2025-04-01T00:00:00Z"
        event_name: buy
        amount: 1000
      - user_id: "u000002"
        client_event_timestamp: "2025-04-01T00:01:00Z"
        event_name: sell
        amount: 3000
      - user_id: "u000003"
        client_event_timestamp: "2025-04-01T00:02:00Z"
        event_name: pay
        amount: 500
  schema:
    fields:
      - name: user_id
        type: string
      - name: client_event_timestamp
        type: timestamp
      - name: event_name
        type: string
      - name: amount
        type: int64
sinks:
  name: auxia_sink
  module: auxia
  inputs:
    - input
  parameters:
    project_id: "0000"
    pubsub:
      topic: projects/xxx/topics/yyy
```

In the above example pipeline, the following data is integrated to Auxia based on [schema](https://docs.auxia.io/data-ingestion/overview/user-event-data)
```json
[
  {
    "project_id": "0000",
    "user_id": "u000001",
    "events": [
      {
        "event_name": "buy",
        "client_event_timestamp": "2025-04-01T00:00:00Z",
        "event_properties": {
          "amount": {
            "long_value": 1000
          }
        }
      }
    ]
  },
  {
    "project_id": "0000",
    "user_id": "u000002",
    "events": [
      {
        "event_name": "sell",
        "client_event_timestamp": "2025-04-01T00:01:00Z",
        "event_properties": {
          "amount": {
            "long_value": 3000
          }
        }
      }
    ]
  },
  {
    "project_id": "0000",
    "user_id": "u000003",
    "events": [
      {
        "event_name": "pay",
        "client_event_timestamp": "2025-04-01T00:02:00Z",
        "event_properties": {
          "amount": {
            "long_value": 500
          }
        }
      }
    ]
  }
]
```


#### json schema

Even in the case of JSON, you can integrate in the same way by specifying a `field` with a json value in `type` = `json`
The following setting performs the same conversion as above

```yaml
options:
  streaming: true
sources:
  name: input
  module: create
  parameters:
    elements:
      - value: '{"user_id": "u000001", "event_name": "buy", "client_event_timestamp": "2025-04-01T00:00:00Z", "amount": 1000 }'
      - value: '{"user_id": "u000002", "event_name": "sell", "client_event_timestamp": "2025-04-01T00:01:00Z", "amount": 5000 }'
      - value: '{"user_id": "u000003", "event_name": "pay", "client_event_timestamp": "2025-04-01T00:02:00Z", "amount": 500 }'
  schema:
    fields:
      - name: value
        type: json
sinks:
  name: auxia_sink
  module: auxia
  inputs:
    - input
  parameters:
    type: json
    project_id: "0000"
    field: value
    pubsub:
      topic: projects/xxx/topics/yyy
```
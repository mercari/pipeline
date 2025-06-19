# PubSub Failure Module

PubSub failure module publishes failure record to the specified PubSub topic.

## Failure module common parameters

| parameter         | optional | type                     | description                                                                                                                                                                                              |
|-------------------|----------|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name              | required | String                   | Step name. specified to be unique in config file.                                                                                                                                                        |
| module            | required | String                   | Specified `pubsub`                                                                                                                                                                                       |
| parameters        | required | Map<String,Object\>      | Specify the following individual parameters.                                                                                                                                                             |

## PubSub failure module parameters

| parameter           | optional | type           | description                                                                                                                                                                                                                                                                         |
|---------------------|----------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic               | required | String         | Specify the PubSub topic name of the destination in the following format. `projects/{gcp project}/topics/{topic name}`                                                                                                                                                              |
| format              | optional | Enum           | Specifies the format of the message to publish. Currently supporting `json`, `avro`. The default is `json`.                                                                                                                                                                         |
| idAttribute         | optional | String         | Specify the attribute name when you want to give an [ID](https://cloud.google.com/dataflow/docs/concepts/streaming-with-cloud-pubsub#efficient_deduplication) to a message as attribute value. The value of the record of the field with the name specified here is used as the ID. |
| timestampAttribute  | optional | String         | Specify the attribute name when you want to save the event time of the record as attribute value.                                                                                                                                                                                   |
| maxBatchSize        | optional | Integer        | Specify the number of buffers to send to PubSub at one time.                                                                                                                                                                                                                        |
| maxBatchBytesSize   | optional | Integer        | Specifies the buffer byte size to be sent to PubSub at one time.                                                                                                                                                                                                                    |

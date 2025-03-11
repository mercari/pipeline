# Aggregation Transform Module (Experimental)

Aggregation transform module performs the specified aggregation process with multiple inputs.

The same aggregate processing can be performed in batch and streaming.

Compared to `beamsql` module, `aggregation` module specializes in aggregate processing and has simpler functionality but better performance.
Also, functions that are not available in `beamsql`, such as `last`, `first`, `argmax`, `argminn`, can be used.

### Output schema

If the `select` parameter is not specified, the schema of the output record includes the fields specified in groupFields and the timestamp field representing the event time of the record, in addition to the processing result fields of `aggregations`.
If the `select` parameter is specified, only the processing result fields of selectField are output.

## Transform module common parameters

| parameter  | optional | type                                        | description                                       |
|------------|----------|---------------------------------------------|---------------------------------------------------|
| name       | required | String                                      | Step name. specified to be unique in config file. |
| module     | required | String                                      | Specified `aggregation`                           |
| inputs     | required | Array<String\>                              | Specify the input names to be aggregated.         |
| parameters | required | Map<String,Object\>                         | Specify the following individual parameters.      |
| strategy   | optional | [Windowing Strategy](../common/strategy.md) | Specify the following individual parameters.      |

## Aggregation transform module parameters

The output schema will include the fields specified in `groupFields`, the fields with the names specified in `aggregations[].fields[].name`, and the timestamp field indicating the date and time of the aggregation.

(If true is specified in `outputPaneInfo`, additional fields will be added to the output schema)

| parameter        | optional | type                                   | description                                                                                                                                                                                  |
|------------------|----------|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| aggregations     | required | Array<Aggregation\>                    | Specify the definition of the aggregate process for each input.                                                                                                                              |
| groupFields      | optional | Array<String\>                         | Specify the names of fields to be referenced to group the data.　                                                                                                                             |
| filter           | optional | [FilterCondition](../common/filter.md) | Specify filter conditions if you want to filter the records of the aggregate processing results.                                                                                             |
| select           | optional | Array<[SelectField](select.md)\>       | Specify the field definitions if you want to refine, rename, or apply some processing to the fields in the aggregate processing results.                                                     |
| limit            | optional | Limit                                  | Specify the limiting conditions for outputting aggregate results. This restriction is applied on a by key basis.                                                                             |
| fanout           | optional | Integer                                | Specify number of intermediate node to combine parts of the data to reduce load on the final global combine step                                                                             |
| outputEmpty      | optional | Boolean                                | Specify whether to output even if no data existed during the specified group and window. The default is `false`                                                                              |
| outputPaneInfo   | optional | Boolean                                | Specify true if you want to output pane information when trigger is set in the streaming processing. The default is `false`                                                                  |


## Aggregation parameters

| parameter         | optional           | type                     | description                                          |
|-------------------|--------------------|--------------------------|------------------------------------------------------|
| input             | required           | String                   | Specify a input name to be processed for aggregation |
| fields            | required           | Array<AggregationField\> | Specify the definitions of the aggregation process   |

## AggregationField common parameters

| parameter | optional | type                                   | description                                                                                                                                |
|-----------|----------|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String                                 | Specify the name of the field in the aggregate result. Must be unique.                                                                     |
| op        | required | Enum                                   | Specify the aggregate process type. Parameters differ depending on the `op`. Refer to following table of supported aggregation operations. |
| condition | optional | [FilterCondition](../common/filter.md) | Specify filter conditions to narrow down the data to be aggregated.                                                                        |
| ignore    | optional | Boolean                                | Specify true if you do not want to execute this aggregate processing                                                                       |

### Supported aggregation operations

| op         | description                                                                                                                                                                                                                                   | additional parameters                                          |
|------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| count      | Outputs data count                                                                                                                                                                                                                            | -                                                              |
| max        | Outputs the maximum value of the specified `field` or `expression`                                                                                                                                                                            | `field` or `expression`                                        |
| min        | Outputs the minimum value of the specified `field` or `expression`                                                                                                                                                                            | `field` or `expression`                                        |
| last       | Outputs the specified value of `field` or values of `fields` for the last data in the group                                                                                                                                                   | `field` or `fields`                                            |
| first      | Outputs the specified value of `field` or values of `fields` for the first data in the group                                                                                                                                                  | `field` or `expression`                                        |
| sum        | Outputs the sum of the values of the specified `field` or `expression`                                                                                                                                                                        | `field` or `expression`                                        |
| avg        | Outputs the average of the values of the specified `field` or `expression`. If you want to produce a weighted average, specify the name of the field with the weight values as `weightField`                                                  | `field` or `expression`, `weightField`                         |
| std        | Outputs the stddev of the values of the specified `field` or `expression`.                                                                                                                                                                    | `field` or `expression`, `ddof`                                |
| argmax     | Outputs the value of the specified `field` or `fields` for the data with the highest value of the specified `comparingField` or `comaringExpression`                                                                                          | `field` or `fields`, `comparingField` or `comparingExpression` |
| argmin     | Outputs the value of the specified `field` or `fields` for the data with the lowest value of the specified `comparingField` or `comaringExpression`                                                                                           | `field` or `fields`, `comparingField` or `comparingExpression` |
| regression | Outputs the slope and intercept and RMSE of a linear simple regression with specified field as the objective variable. The field for the explanatory variable is specified by `xField`. If not specified, epoch millis of record will be used | `field`, `xField`                                              |
| array_agg  | Outputs the values of the specified `field` in an array. If multiple `fields` are specified, it will be an array of structs.                                                                                                                  | `field` or `fields`                                            |

## Limit parameters

| parameter     | optional           | type      | description                                                                                                                  |
|---------------|--------------------|-----------|------------------------------------------------------------------------------------------------------------------------------|
| count         | selective required | Integer   | Specifies the number of outputs to limit for each key. The specified number of outputs will be output from oldest to newest. |

## Related example config files

* [BigQuery to Aggregation to BigQuery](../../../../examples/bigquery-to-aggregation-to-bigquery.json)
* [PubSub to Aggrergation to PubSub](../../../../examples/pubsub-to-aggregation-to-pubsub.json)

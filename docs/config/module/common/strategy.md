# Windowing Strategy

The Strategy parameter defines the [window strategy]([Beam's official documentation](https://beam.apache.org/documentation/programming-guide/#windowing) ) for the specified module.

## Parameters

| parameter        | optional | type    | description                                                                                                                                                                                  |
|------------------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| window           | optional | Window  | Specify [window](https://beam.apache.org/documentation/programming-guide/#windowing) by time to aggregate data. The default is `global` window.                                              |
| trigger          | optional | Trigger | Specify window [trigger](https://beam.apache.org/documentation/programming-guide/#setting-a-trigger) setting. The default is `afterWatermark` trigger                                        |
| accumulationMode | optional | Enum    | Specify trigger [accumulation mode](https://beam.apache.org/documentation/programming-guide/#window-accumulation-modes). One of `discarding` or `accumulating`. the default is `discarding`. |

## Window parameters

| parameter         | optional           | type    | description                                                                                                                                                                       |
|-------------------|--------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type              | required           | Enum    | Window [type](https://beam.apache.org/documentation/programming-guide/#provided-windowing-functions). One of `global`, `fixed`, `sliding`, `session`, or `calendar`.              |
| unit              | optional           | Enum    | Window size unit. One of `second`, `minute`, `hour`, `day`, `week`, `month`, or `year`. The default is `second`.                                                                  |
| size              | selective required | Integer | Window size. required if type is `fixed` or `sliding` or `calendar`                                                                                                               |
| period            | selective required | Integer | Window trigger period. required if type is `sliding`. the unit is the same as specified in `unit`.                                                                                |
| gap               | selective required | Integer | Window trigger gap. required if type is `session`.                                                                                                                                |
| offset            | optional           | Integer | Window offset (for `fixed` or `sliding` window). the unit is the same as specified in `unit`.                                                                                     |
| timezone          | optional           | String  | Specify timezone if type is `calendar`. The default is `UTC`                                                                                                                      |
| startingDate      | optional           | Date    | Specify starting date if type is `calendar` and you want to specify a starting point.                                                                                             |
| allowedLateness   | optional           | Integer | Define the tokenizing process for each field of the input record.                                                                                                                 |
| timestampCombiner | optional           | Enum    | Specify how the timestamp value of the output is calculated when an early trigger is set. specify one of `EARLIEST`, `LATEST`, or `END_OF_WINDOW`. The default is `END_OF_WINDOW` |

## Trigger parameters

This setting specifies the [trigger for the window](https://beam.apache.org/documentation/programming-guide/#triggers).

| parameter                 | optional           | type            | description                                                                                                                                                                                                                              |
|---------------------------|--------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                      | required           | Enum            | Trigger [type](https://beam.apache.org/documentation/programming-guide/#triggers). One of `afterWatermark`, `afterProcessingTime`, `afterPane`, `repeatedly`, `afterEach`, `afterFirst`, or `afterAll`.                                  |
| earlyFiringTrigger        | optional           | Trigger         | (For `afterWatermark` trigger) Specify the trigger that you want to fire before the watermark.                                                                                                                                           |
| lateFiringTrigger         | optional           | Trigger         | (For `afterWatermark` trigger) Specify the trigger that you want to fire after the watermark.                                                                                                                                            |
| childrenTriggers          | selective required | Array<Trigger\> | (For [composite triggers](https://beam.apache.org/documentation/programming-guide/#composite-triggers) such as `afterEach`, `afterFirst`, `afterAll` trigger) Specify triggers that are the firing conditions for the composite trigger. |
| foreverTrigger            | selective required | Trigger         | (For `repeatedly` trigger) specifies a trigger that executes forever.                                                                                                                                                                    |
| finalTrigger              | optional           | Trigger         | Specify a trigger for final condition to cause any trigger to fire one final time and never fire again                                                                                                                                   |
| pastFirstElementDelay     | selective required | Integer         | (For `afterProcessingTime` trigger) Specify the interval of time that has elapsed since the arrival of the first data.                                                                                                                   |
| pastFirstElementDelayUnit | selective required | Enum            | (For `afterProcessingTime` trigger) Specify the unit of pastFirstElementDelay. One of `second`, `minute`, `hour`, or `day`. The default is `second`.                                                                                     |
| elementCountAtLeast       | selective required | Integer         | (For `afterPane` trigger) Specify the number of data to be the firing condition                                                                                                                                                          |



## Related example config files

* [BigQuery to Cloud Storage(Parquet)](../../../../examples/bigquery-to-parquet.json)
* [Cloud Datastore to Cloud Storage(Avro)](../../../../examples/datastore-to-avro.json)

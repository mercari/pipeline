# Create Source Module

Create Module generates data with specified conditions

## Source module common parameters

| parameter          | optional | type                | description                                                                                                                   |
|--------------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------------------------|
| name               | required | String              | Step name. specified to be unique in config file.                                                                             |
| module             | required | String              | Specified `create`                                                                                                            |
| schema             | optional | [Schema](SCHEMA.md) | Schema of the data to be read. (you don't need to specify it.)                                                                |
| timestampAttribute | optional | String              | If you want to use the value of an field as the event time, specify the name of the field. (The field must be Timestamp type) |
| parameters         | required | Map<String,Object\> | Specify the following individual parameters                                                                                   |

## Create source module parameters

`create` module can specify data directly in `elements`, or it can generate data by specifying a range `from` and `to`.

You can also process the generated data by specifying a `select` parameter.

| parameter    | optional           | type                                       | description                                                                                                                                                                                    |
|--------------|--------------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type         | required           | Enum                                       | Specify the type of data listed in `elements`, or data to be generated within the range specified by `from` (and `to`). `string`, `long`, `double`, `date`, `time`, `timestamp` are supported. |
| elements     | selective required | Array<?\>                                  | List the data you need to process in following modules.　                                                                                                                                       |
| from         | selective required | String                                     | Specify the initial value of the range of data to be generated.　                                                                                                                               |
| to           | optional           | String                                     | Specify the last value in the range of data to be generated. If not specified, the data will be generated indefinitely (only streaming mode).　                                                 |
| interval     | optional           | Integer                                    | Specify the interval for generating the range of data specified by `from` and `to`.                                                                                                            |
| intervalUnit | optional           | Enum                                       | If type is `date`, `time`, or `timestamp`, specify the unit of interval. `second`, `minute`, `hour`, `day`, `week`, `month`, `year` are supported.                                             |
| rate         | optional           | Float                                      | Specify the frequency of generation per unit of time when executing in streaming mode.                                                                                                         |
| rateUnit     | optional           | Enum                                       | Specifies the unit of time when `rate` is specified.  `second`, `minute`, `hour` are supported.                                                                                                |
| select       | optional           | Array<[SelectField](../common/select.md)\> | Specify the field definitions if you want to refine, rename, or apply some processing to the generated data.                                                                                   |

### Output Schema

The schema of the output of this module is that if no `select` parameter is specified, the output will be following schema.

| name      | type                  | description                                               |
|-----------|-----------------------|-----------------------------------------------------------|
| sequence  | Integer               | The sequence of generated data is output in order from 1. |
| value     | (specified by `type`) | Generated data                                            |
| timestamp | Timestamp             | Processed timestamp                                       |

## Related example config files

* [Create to Solr on CloudRun to BigQuery](../../../../examples/create-to-http-to-bigquery.json)

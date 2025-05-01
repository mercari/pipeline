# SelectField

SelectField is the definition for limiting the fields to be output, changing field names, and making slight modifications.

## SelectField common parameters

| parameter | optional | type    | description                                                                                                                  |
|-----------|----------|---------|------------------------------------------------------------------------------------------------------------------------------|
| name      | required | String  | Specify the name of the field in the aggregate result. Must be unique.                                                       |
| func      | optional | Enum    | Specify the processing function. Parameters differ depending on the `func`. Refer to following table of supported functions. |
| ignore    | optional | Boolean | Specify true if you do not want to execute this select processing                                                            |

## Supported Select functions

### Stateless functions

`pass`, `rename`, `cast`, `constant`, and `expression` can omit parameter `func`.
(It is automatically inferred from the other parameters specified)

| func              | description                                                                                                                                           | additional parameters        |
|-------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|
| pass              | Holds the value of the field specified by `name`                                                                                                      | -                            |
| rename            | Renames the specified `field` to the specified `name`.                                                                                                | `field`                      |
| cast              | Cast the data type of the field specified by `name` (or `field` if you want to change the field name too) to the type specified by `type`.            | `type`, `field`              |
| constant          | Generates a field with the specified `type` and `value`. As type values, `boolean`, `string`, `long`, `double`, `date` and `timestamp` are supported. | `type`, `value`              |
| expression        | Embeds input data in the formula specified by the `expression` parameter and outputs the result of the calculation as a double type.                  | `expression`                 |
| text              | Generates text by embedding input data in the template specified by the `text` parameter.                                                             | `text`                       |
| event_timestamp   | Generates a field with a event timestamp value                                                                                                        | -                            |
| current_timestamp | Generates a field with a current timestamp value                                                                                                      | -                            |
| concat            | Concatenates values of the specified `fields` as a string. if `delimiter` is specified, it will be combined using the value.                          | `fields`, `delimiter`        |
| uuid              | Generates a field with uuid string value                                                                                                              | -                            |
| hash              | Generates a hashed string of the values of the specified `fields` as a string. if `size` is specified, returns it in the length of the string.        | (`fields` or `text`), `size` |
| struct            | Generate nested structure field by defining the `fields` of select. If you want to generate an array of structures, specify `repeated` in `mode`.     | `fields`, `mode`, `each`     |
| json              | Generate nested json field by defining the `fields` of select. If you want to generate an array of structures, specify `repeated` in `mode`.          | `fields`, `mode`, `each`     |
| jsonpath          | Extract STRING value based on the JSON PATH specified in `path` for the value in the specified `field`.                                               | `field`, `path`              |
| base64_encode     | Encodes the value of the specified `field` in Base64 format and converts it to a byte array.                                                          | `field`                      |
| base64_decode     | Decodes the value of the specified `field` in Base64 format and converts it to a byte array.                                                          | `field`                      |

### Stateful Aggregation functions

The stateful function can specify a range relative to the input records targeted by the aggregate as a range.
The range is specified in the order sorted by event time.

| parameters | optional           | type    | description                                                                                                 |
|------------|--------------------|---------|-------------------------------------------------------------------------------------------------------------|
| count      | selective required | Integer | The number of pieces to include in the aggregate, relative to the input records going back in time          |
| duration   | selective required | Integer | The period of time from the input record to be included in the aggregate (time unit is specified by “unit”) |
| unit       | optional           | Enum    | The unit of time for the period, selected from `second`, `minute`, `hour`, `day`. the default is second     |


| func       | description                                                                                                                                                                                                                                   | additional parameters                                          |
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
| array_agg  | Outputs the values of the specified `field` in an array. If multiple `fields` are specified, it will be an array of structs.                                                                                                                  | `field` or `fields`                                            |
| regression | Outputs the slope and intercept and RMSE of a linear simple regression with specified field as the objective variable. The field for the explanatory variable is specified by `xField`. If not specified, epoch millis of record will be used | `field`, `xField`                                              |

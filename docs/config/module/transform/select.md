# Select Transform Module

Select transform module can be used to filter rows by specified filter condition and process field values by specified select condition.

## Transform module common parameters

| parameter  | optional | type                                        | description                                                                                                 |
|------------|----------|---------------------------------------------|-------------------------------------------------------------------------------------------------------------|
| name       | required | String                                      | Step name. specified to be unique in config file.                                                           |
| module     | required | String                                      | Specified `select`                                                                                          |
| inputs     | required | Array<String\>                              | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\>                         | Specify the following individual parameters.                                                                |
| strategy   | optional | [Windowing Strategy](../common/strategy.md) | Specify [windowing strategy](https://beam.apache.org/documentation/programming-guide/#windowing)            |

## Filter transform module parameters

| parameter   | optional           | type                                       | description                                                                                                      |
|-------------|--------------------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| filter      | selective required | [FilterCondition](../common/filter.md)     | Specify the conditions for filtering rows.                                                                       |
| select      | selective required | Array<[SelectField](../common/select.md)\> | Specify a list of field definitions if you want to refine, rename, or apply some processing to the input fields. |
| groupFields | optional           | Array<String\>                             | Specify the names of fields to be referenced to group the data.　                                                 |


## Related example config files

* [Avro to filter to Avro](../../../../examples/avro-to-filter-to-avro.json)

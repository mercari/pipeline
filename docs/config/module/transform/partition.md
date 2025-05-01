# Partition Transform Module

Partition transform module splits a data collection into separate data collections based on conditions.

## Transform module common parameters

| parameter  | optional | type                | description                                                                                                 |
|------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                                                           |
| module     | required | String              | Specified `partition`                                                                                       |
| inputs     | required | Array<String\>      | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                                |

## Partition transform module parameters

| parameter  | optional | type                   | description                                                                          |
|------------|----------|------------------------|--------------------------------------------------------------------------------------|
| partitions | required | Array<PartitionField\> | Specify the conditions to be assigned to each partition.                             |
| exclusive  | optional | Boolean                | Specify whether the partitions should be allocated exclusively. The default is true. |
| separator  | optional | String                 | Specify the separator that joins the module name and input names. The default is `.` |

## PartitionField parameters

| parameter | optional | type                                       | description                                                                                                                                                                                                             |
|-----------|----------|--------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| output    | required | String                                     | Specifies the output name of the partition. (given as a suffix to the module name). Records that do not match any of the filter criteria for any of the partitions will be associated with the output named `defaults`. |
| filter    | required | [FilterCondition](../common/filter.md)     | Specify the conditions for assigning data to a partition.                                                                                                                                                               |
| select    | optional | Array<[SelectField](../common/select.md)\> | Specify a list of field definitions if you want to refine, rename, or apply some processing to the source fields.                                                                                                       |

## Related example config files

* [Split Avro records to Cloud Spanner](../../../../examples/avro-to-partition-to-spanner.json)

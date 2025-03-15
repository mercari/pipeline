# Debug Sink Module

Sink module for outputting specified data to the log.

## Sink module common parameters

| parameter  | optional | type                | description                                       |
|------------|----------|---------------------|---------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file. |
| module     | required | String              | Specified `debug`                                 |
| inputs     | required | Array<String\>      | Step names whose data you want to print log       |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.      |

## Debug sink module parameters

| parameter   | optional | type   | description                                                                                                                                                                                                                                                                         |
|-------------|----------|--------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| logLevel    | optional | String | Log level. You can specify either `trace`,`debug`,`info`,`warn` or `error`. default is `debug`.                                                                                                                                                                                     |
| logTemplate | optional | String | When you want to embed the text in a template in [FreeMarker format](https://freemarker.apache.org/), specify the text to be the template. the variables that can be used from the template are as follows. If nothing is specified, each data is output to the log in JSON format. |

## Variables available in Template

| name               | type   |
|--------------------|--------|
| data               | String |
| timestamp          | String |
| paneTiming         | String |
| paneIsFirst        | String |
| paneIsLast         | String |
| paneIndex          | String |
| windowMaxTimestamp | -      |
| windowStart        | String |
| windowEnd          | String |

# Http Transform Module (Experimental)

Http transform module can be used to filter rows by specified criteria or to retrieve only specified columns.

## Transform module common parameters

| parameter  | optional | type                | description                                                                                                 |
|------------|----------|---------------------|-------------------------------------------------------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.                                                           |
| module     | required | String              | Specified `http`                                                                                            |
| inputs     | required | Array<String\>      | Specify the names of the step from which you want to process the data, including the name of the transform. |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                                                                |

## Http transform module parameters

| parameter      | optional | type                                       | description                                                                                                                 |
|----------------|----------|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| request        | required | Request                                    | Specify the configuration settings needed to create an HTTP request                                                         |
| response       | required | Response                                   | Specify the configuration options needed to parse the HTTP response                                                         |
| select         | optional | Array<[SelectField](../common/select.md)\> | Specify the field definitions if you want to refine, rename, or apply some processing to the http response or input fields. |
| timeoutSeconds | optional | Integer                                    | Specify the seconds in which a request is considered to have timed out and failed. The default is 60.                       |

## Http transform module Request parameters

| parameter | optional | type                             | description                                                                                                                                                                                                                                |
|-----------|----------|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| endpoint  | required | String                           | Specify the url for http access. You can embed the fields of the input record with Template Engine FreeMarker.                                                                                                                             |
| method    | required | Enum                             | Specify one of `get`, `post`, `put`, or `delete` as the http request method                                                                                                                                                                |
| body      | optional | String                           | Specify the body of the http request. You can embed the fields of the input record with Template Engine FreeMarker                                                                                                                         |
| params    | optional | Map<String,String\>              | Specify the request parameters as map. You can embed the fields of the inputs with Template Engine FreeMarker. `                                                                                                                           |
| headers   | optional | Map<String,String\>              | Specify the headers of the http request. You can embed the fields of the inputs with Template Engine FreeMarker. `${_access_token}`, and `${_id_token}` variables in the headers value can be used to embed the dataflow worker's SA token |

## Http transform module Response parameters

| parameter             | optional  | type                          | description                                                                                                                                                                                                                                     |
|-----------------------|-----------|-------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| format                | required  | Enum                          | Specify `text`, `bytes`, or `json` as the http response format                                                                                                                                                                                  |
| schema                | optional  | [Schema](../source/SCHEMA.md) | Specify the schema for response body validation. If specified and the schema does not match, it will be treated as an error.                                                                                                                    |
| acceptableStatusCodes | optional  | List<Integer\>                | By default, 4xx series statuses are treated as errors. 5xx series statuses are also treated as errors, but are retried a certain number of times. If you have a status code that you want to be treated as successful, specify the status codes |

The schema of the output of this module is that if no `select` parameter is specified, the output will be following schema.
You can also specify fields to be extracted from this `body` field or from the fields of the input record by using `select` parameter.

### Succeed record schema

| name       | type                          | description                                                   |
|------------|-------------------------------|---------------------------------------------------------------|
| statusCode | Integer                       | response status code                                          |
| body       | String, Bytes, Json or Struct | response body. Type depends on `format` and `response.schema` |
| headers    | Map<String,List<String\>\>    | response headers                                              |
| timestamp  | Timestamp                     | Processed timestamp                                           |

Records that fail to be processed by this module are output to `{module step name}.failures` with the following schema If necessary, save it to BQ or other file for recovery or problem handling.

### Failure record schema

| name       | type      | description                                      |
|------------|-----------|--------------------------------------------------|
| message    | String    | error message text                               |
| request    | String    | The failed input record is output in text format |
| stackTrace | String    | stackTrace text                                  |
| timestamp  | Timestamp | Processed timestamp                              |

## Related example config files

* [Pub/Sub to HttpRequest to Pub/Sub](../../../../examples/pubsub-to-http-to-pubsub.json)

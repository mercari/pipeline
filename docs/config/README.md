# Define Pipeline

## Config file contents

In the Config file, six modules, `system`, `options`, `sources`, `transforms`, `sinks` and `failures`, are combined to define the processing contents.
`sources` is for input data acquisition, `transforms` is for data processing, and `sinks` is for data output.
`options` defines pipeline options.

| parameter  | type                         | description                            |
|------------|------------------------------|----------------------------------------|
| system     | [System](system.md)          | System configuration.                  |
| options    | [Options](options/README.md) | Pipeline option definitions.           |
| sources    | Array<Source\>               | Pipeline data source definitions.      |
| transforms | Array<Transform\>            | Pipeline data processing definitions.  |
| sinks      | Array<Sink\>                 | Pipeline data sink definitions.        |
| failures   | Array<Failure\>              | Pipeline dead-letter sink definitions. |


```JSON:config
{
  "system": {...},
  "options": {...},
  "sources": [
    {...},
    ...
  ],
  "transforms": [
    {...},
    ...
  ],
  "sinks": [
    {...},
    ...
  ],
  "failures": [
    {...},
    ...
  ]
}
```

You can define and run a pipeline by combining these three types of various build-in modules.

The list of build-in modules can be found on [Modules Page](module/README.md).

Examples of configuration files are listed in the [Examples Page](../../examples/README.md), so try to find and arrange a configuration file that is close to the data processing you want to perform.

Below is an overview of these built-in modules.

## Module common attributes

In the three types of modules, the contents of input, processing, and output are described as JSON parameters.
The common settings of the three types of modules are as follows.

| attribute  | type                 | optional | description                                                                                                                                                                                                                                                                                         |
|------------|----------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| name       | String               | required | Set unique name in Config JSON                                                                                                                                                                                                                                                                      |
| module     | String               | required | Set [module](module/README.md) name                                                                                                                                                                                                                                                                 |
| parameters | Map<String, Object\> | required | Specify the parameters defined in each module.                                                                                                                                                                                                                                                      |
| strategy   | Strategy             | optional | Specify beam windowing strategy([Window](https://beam.apache.org/documentation/programming-guide/#windowing), [Trigger](https://beam.apache.org/documentation/programming-guide/#triggers), [AccumulationMode](https://beam.apache.org/documentation/programming-guide/#window-accumulation-modes)) |
| waits      | Array<String\>       | optional | If you want to wait for the completion of other steps and then start this step, assign a step Name to wait for completion.                                                                                                                                                                          |
| failFast   | Boolean              | optional | Specify true if you want the job to fail immediately when an error occurs. The default is true for batch and false for streaming.                                                                                                                                                                   |
| ignore     | Boolean              | optional | Specify true if you want to ignore this module.                                                                                                                                                                                                                                                     |


### Module Common Properties Matrix

|                    | source   | transform | sink     |
|--------------------|----------|-----------|----------|
| name               | required | required  | required |
| module             | required | required  | required |
| parameters         | required | required  | required |
| inputs             | -        | required  | required |
| sideInputs         | -        | optional  | optional |
| waits              | -        | optional  | optional |
| schema             | optional | -         | optional |
| strategy           | -        | optional  | optional |
| timestampAttribute | optional | -         | -        |
| failFast           | optional | optional  | optional |
| ignore             | optional | optional  | optional |


## Source modules

The source module defines the source of the data you want to process in the pipeline.
Common configuration items in the source module are as follows.

| parameter          | type                              | optional | description                                                                                                                                                                    |
|--------------------|-----------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| schema             | [Schema](module/source/SCHEMA.md) | optional | Specifies the schema of the input resource. If the input resource has schema information, no specification is required.                                                        |
| timestampAttribute | String                            | optional | Defines which fields of the source record should be treated as EventTime. The default is the time of input.                                                                    |


## Transform modules

The transform module defines what to do with the data.
The common settings of the transform module are as follows.

| parameter  | type           | optional | description                                                                                                   |
|------------|----------------|----------|---------------------------------------------------------------------------------------------------------------|
| inputs     | Array<String\> | required | Specify the names of the module from which you want to process the data, including the name of the transform. |
| sideInputs | Array<String\> | optional | Specify the name of the input when additional information is needed for processing.                           |


## Sink modules

The sink module defines the output destination of the data.
The common settings of the sink module are as follows

| parameter  | type           | optional | description                                                                                                                 |
|------------|----------------|----------|-----------------------------------------------------------------------------------------------------------------------------|
| inputs     | Array<String\> | required | Specify the name of the module from which you want to output data. source or transform name.                                |
| sideInputs | Array<String\> | optional | Specify the name of the input when additional information is needed for writing.                                            |

## Rewriting the configuration file at runtime

In the configuration file, you can use the Template Engine, [Apache FreeMarker](https://freemarker.apache.org/), to assign variables at runtime, or you can even rewrite the file itself.

You can define variables in the configuration file, as in the example below, and assign values at run time.
The notation follows the FreeMarker specification.

```JSON
{
  "sources": [
    {
      "name": "MyKindInput",
      "module": "datastore",
      "timestampAttribute": "created_at",
      "schema": {
        "fields": []
      },
      "parameters": {
        "projectId": "myproject",
        "gql": "SELECT * FROM MyKind WHERE created_at > DATETIME('${current_datetime}')"
      }
    }
  ],
  "sinks": [
    {
      "name": "MyKindOutput",
      "module": "storage",
      "input": "MyKindInput",
      "parameters": {
        "output": "${output_path}",
        "format": "avro"
      }
    }
  ]
}
```

You can assign variables to the Config file at runtime by prefixing it with the parameter `template.`.

```sh
gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json} \
  --parameters=args.current_datetime=2020-12-01T00:00:00Z \
  --parameters=args.output_path=gs://mybucket/output
```

REST API version

```sh
CONFIG="$(cat examples/xxxx.json)"
curl -X POST -H "Content-Type: application/json"  -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/templates:launch"`
  `"?dynamicTemplate.gcsPath=gs://{path/to/legacy_template_file}" -d "{
    'parameters': {
      'config': '$(echo "$CONFIG")',
      'args.current_datetime': '2020-12-01T00:00:00Z',
      'args.output_path': 'gs://mybucket/output',
    },
    'jobName':'myJobName',
  }"
```
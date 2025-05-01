# Onnx Transform Module (Experimental)

Onnx module uses the specified onnx file to perform inference on the input data.

The same processing can be performed in batch and streaming.

Some onnx file sizes require large memory space. When using large onnx files, it is recommended to use multi-core workers with large memory. This is because onnx model is shared by multiple processes in memory.

## Transform module common parameters

| parameter  | optional | type                | description                                                   |
|------------|----------|---------------------|---------------------------------------------------------------|
| name       | required | String              | Step name. specified to be unique in config file.             |
| module     | required | String              | Specified `onnx`                                              |
| inputs     | required | Array<String\>      | Specify input step names to apply inference by the onnx model |
| parameters | required | Map<String,Object\> | Specify the following individual parameters.                  |

## Onnx transform module parameters

| parameter   | optional | type              | description                                                                       |
|-------------|----------|-------------------|-----------------------------------------------------------------------------------|
| model       | required | Model             | Settings related to the onnx model used for inference                             |
| inferences  | required | Array<Inference\> | Define input-output mapping and post-processing for inference with the onnx model |

## Model parameters

One onnx model can be defined in one onnx transform module.

| parameter          | optional | type                                        | description                                                                                                                                                                                                                                                                                                                     |
|--------------------|----------|---------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path               | required | String                                      | GCS path of the onnx file to be used for inference                                                                                                                                                                                                                                                                              |
| optLevel           | optional | Enum                                        | The [optimisation](https://onnxruntime.ai/docs/performance/model-optimizations/graph-optimizations.html) level to use. `BASIC_OPT`, `ALL_OPT`, `EXTENDED_OPT`, and `NO_OPT` options can be specified. The default is `BASIC_OP`                                                                                                 |
| outputSchemaFields | optional | Array<[Schema.Field](../source/SCHEMA.md)\> | Specify the fields of the schema to be output by the onnx model. If specified, the process of downloading the onnx file and obtaining the schema at startup can be omitted. (Since loading a large onnx file requires a large amount of memory for the worker to start up, it is recommended to specify this field if possible) |

## Inference parameters

A single onnx module can define inferences for multiple inputs. In addition, multiple inferences can be defined for a single input.
Define input-output mapping and post-processing for each input step.

| parameter | optional | type                                       | description                                                                                                            |
|-----------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| input     | required | String                                     | Input step name to which you want to apply inference with the specified model                                          |
| mappings  | required | Array<Mapping\>                            | Specify the mapping between the input/output of the specified model and the field names of the module's input/output.  |
| select    | optional | Array<[SelectField](../common/select.md)\> | Specify the field definitions if you want to refine, rename, or apply some processing to the fields inference results. |

## Mapping parameters

| parameter | optional | type                | description                                                                                                                                                                                                            |
|-----------|----------|---------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| inputs    | required | Map<String,String\> | Specify the field names of the input steps to be applied to the input names of the onnx model. The Map key is the input name of the onnx model.                                                                        |
| outputs   | required | Map<String,String\> | If you want to change the output field name from the output name of the onnx model, specify that field name. The key of the map is the output name of the onnx model and the value is the field name after the change. |

## Related example config files

* [BigQuery to ONNX to Vertex AI Vector Search](../../../../examples/bigquery-to-onnx-to-vectorsearch.json)

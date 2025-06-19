# Define pipeline options

options defines pipeline settings.
The following items can be defined as options.

| parameter | type                            | description                                                                                                |
|-----------|---------------------------------|------------------------------------------------------------------------------------------------------------|
| jobName   | String                          | jobName.                                                                                                   |
| streaming | Boolean                         | Specify whether the dataflow job starts in streaming mode or not.                                          |
| dataflow  | [Dataflow Options](dataflow.md) | Specify [Cloud Dataflow runner](https://beam.apache.org/documentation/runners/dataflow/) specific options. |
| direct    | [Direct Options](direct.md)     | Specify [Direct runner](https://beam.apache.org/documentation/runners/direct/) specific options.           |
| gcp       | [GCP Options](gcp.md)           | Specify Google Cloud options.                                                                              |


#### Example

```JSON:options
{
  "options": {
    "streaming": true,
    "dataflow": {
      "workerMachineType": "n2-custom-2-131072-ext",
      "numWorkers": 1,
      "diskSizeGb": 256,
      "workerDiskType": "compute.googleapis.com/projects//zones//diskTypes/pd-ssd"
    },
    "beamsql": {
      "plannerName": "org.apache.beam.sdk.extensions.sql.impl.CalciteQueryPlanner"
    }
  },
  "sources": [],
  "transforms": [],
  "sinks": []   
}
```

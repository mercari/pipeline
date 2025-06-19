# Execute Mercari Pipeline

## gcloud command

gcloud command allows you to execute a configuration file uploaded to GCS with parameters as follows

```sh
gsutil cp config.json gs://{path/to/config.json}

gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json}
```

## REST API

You can also run template by [REST API](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch).

In the following example, instead of uploading the config file to GCS, the contents are specified directly from a local file.
If you want to specify the contents of the config file directly via REST API, you should be aware that you need to escape the JSON string in the config file.

```sh
PROJECT_ID=[PROJECT_ID]
REGION=[REGION]
CONFIG="$(cat examples/xxx.json)"

curl -X POST -H "Content-Type: application/json"  -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${REGION}/flexTemplates:launch" -d "{
  'launchParameter': {
    'jobName': 'myJobName',
    'containerSpecGcsPath': 'gs://{path/to/template_file}',
    'parameters': {
      'config': '$(echo "$CONFIG")',
      'stagingLocation': 'gs://{path/to/staging}'
    },
    'environment': {
      'tempLocation': 'gs://{path/to/temp}'
    }
  }
}"
```

(The options `tempLocation` and `stagingLocation` are optional. If not specified, a bucket named `dataflow-staging-{region}-{project_no}` will be automatically generated and used)

### Run Template in streaming mode

To run Template in streaming mode, specify `streaming=true` in the parameter.

```sh
gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json} \
  --parameters=streaming=true
```

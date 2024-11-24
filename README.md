# Mercari Pipeline

The Mercari Pipeline enables you to run various pipelines without writing programs by simply defining a configuration file.

Mercari Dataflow Template is implemented as a [FlexTemplate](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) for [Cloud Dataflow](https://cloud.google.com/dataflow). Pipelines are assembled based on the defined configuration file and can be executed as Cloud Dataflow Jobs.

See the [Document](docs/README.md) for usage

## Usage Example

Write the following json file and upload it to GCS (Suppose you upload it to gs://example/config.json).

This configuration file stores the BigQuery query results in the table specified by Spanner.

```json
{
  "sources": [
    {
      "name": "bigquery",
      "module": "bigquery",
      "parameters": {
        "query": "SELECT * FROM `myproject.mydataset.mytable`"
      }
    }
  ],
  "sinks": [
    {
      "name": "spanner",
      "module": "spanner",
      "input": "bigquery",
      "parameters": {
        "projectId": "myproject",
        "instanceId": "myinstance",
        "databaseId": "mydatabase",
        "table": "mytable"
      }
    }
  ]
}
```

Assuming you have deployed the Mercari Dataflow Template to gs://example/template, run the following command.

```sh
gcloud dataflow flex-template run bigquery-to-spanner \
  --template-file-gcs-location=gs://example/template \
  --parameters=config=gs://example/config.json
```

The Dataflow job will be started, and you can check the execution status of the job in the console screen.

<img src="https://raw.githubusercontent.com/mercari/DataflowTemplate/master/docs/images/bigquery-to-spanner.png">


## Deploy Template

Mercari Dataflow Template is used as FlexTemplate.
Therefore, the Mercari Dataflow Template should be deployed according to the FlexTemplate creation steps.

### Requirements

* Java 21
* [Maven 3](https://maven.apache.org/index.html)
* [gcloud command-line tool](https://cloud.google.com/sdk/gcloud)

### Push Template Container Image to Cloud Container Registry.

The first step is to build the source code and register it as a container image in the [Cloud Artifact Registry](https://cloud.google.com/artifact-registry).

To upload container images to the Artifact registry via Docker commands, you will first need to execute the following commands, depending on the repository region.

```sh
gcloud auth configure-docker us-central1-docker.pkg.dev, asia-northeast1-docker.pkg.dev
```

The following command will generate a container for FlexTemplate from the source code and upload it to Artifact Registry.

```sh
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/cloud:latest
```

### Upload template file.

The next step is to generate a template file to start a job from the container image and upload it to GCS.

Use the following command to generate a template file that can execute a dataflow job from a container image, and upload it to GCS.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/cloud:latest" \
  --sdk-language "JAVA"
```

## Run dataflow job from template file

Run Dataflow Job from the template file.

* gcloud command

You can run template specifying gcs path that uploaded config file.

```sh
gsutil cp config.json gs://{path/to/config.json}

gcloud dataflow flex-template run {job_name} \
  --template-file-gcs-location=gs://{path/to/template_file} \
  --parameters=config=gs://{path/to/config.json}
```

* REST API

You can also run template by [REST API](https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.flexTemplates/launch).

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

## Deploy Docker image for local pipeline

You can run pipeline locally. This is useful when you want to process small data quickly.

At first, you should register the container for local execution.


```sh
# Generate MDT jar file.
mvn clean package -DskipTests -Dimage="{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/cloud"

# Create Docker image for local run
docker build --tag="{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/local" .

# If you need to push the image to the GAR,
# you may do so by using the following commands
gcloud auth configure-docker
docker push {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/local
```

## Run Pipeline locally

For local execution, execute the following command to grant the necessary permissions

```shell
gcloud auth application-default login
````

The following is an example of a locally executed command.
The authentication file and config file are mounted for access by the container.
The other arguments (such as `project` and `config`) are the same as for normal execution.

If you want to run in streaming mode, specify streaming=true in the argument as you would in normal execution.

### Mac OS

```sh
docker run \
  -v ~/.config/gcloud:/mnt/gcloud:ro \
  -v /{your_work_dir}:/mnt/config:ro \
  --rm {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/local \
  --project={project} \
  --config=/mnt/config/{my_config}.json
```

### Windows OS

```sh
docker run ^
  -v C:\Users\{YourUserName}\AppData\Roaming\gcloud:/mnt/gcloud:ro ^
  -v C:\Users\{YourWorkingDirPath}\:/mnt/config:ro ^
  --rm {region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/local ^
  --project={project} ^
  --config=/mnt/config/{MyConfig}.json
```

* Note:
  * If you use BigQuery module locally, you will need to specify the `tempLocation` argument.
  * If the pipeline is to access an emulator running on a local machine, such as Cloud Spanner, the `--net=host` option is required.

## Committers

 * Yoichi Nagai ([@orfeon](https://github.com/orfeon))

## Contribution

Please read the CLA carefully before submitting your contribution to Mercari.
Under any circumstances, by submitting your contribution, you are deemed to accept and agree to be bound by the terms and conditions of the CLA.

https://www.mercari.com/cla/

## License

Copyright 2024 Mercari, Inc.

Licensed under the MIT License.

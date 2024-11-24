# Deploy Mercari Pipeline

Mercari Pipeline is a portable pipeline tool developed by Apache Beam.
It is deployed as a Docker image for each of data processing frameworks such as Cloud Dataflow, Apache Flink and Apache Spark.

## Requirements

* Java 21
* [Maven 3](https://maven.apache.org/index.html)
* [gcloud command-line tool](https://cloud.google.com/sdk/gcloud)

## Ready for pushing pipeline image to Cloud Artifact Registry.

The first step is to build the source code and register it as a container image to the [Cloud Artifact Registry](https://cloud.google.com/artifact-registry).

To upload container images to the Artifact registry via Docker commands, you will first need to execute the following commands, depending on the repository region.

```sh
gcloud auth login
gcloud auth configure-docker us-central1-docker.pkg.dev, asia-northeast1-docker.pkg.dev
```

## Deploy Cloud Dataflow Flex Template

### Push Docker Image to GAR.

The following command will generate a container for FlexTemplate from the source code and upload it to Artifact Registry.

```sh
mvn clean package -DskipTests -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow
```

### Upload template file.

The next step is to generate a template file to start a job from the container image and upload it to GCS.

Use the following command to generate a template file that can execute a dataflow job from a container image, and upload it to GCS.

```sh
gcloud dataflow flex-template build gs://{path/to/template_file} \
  --image "{region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/dataflow" \
  --sdk-language "JAVA"
```

## Deploy Direct Runner (for local execution)

```sh
mvn clean package -DskipTests -Pdirect -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/direct:latest
```

## Deploy Direct Runner API Server (for deploy API server)

### Push Docker Image to GAR

```sh
mvn clean package -DskipTests -Pserver -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server:latest
```

### Deploy Cloud Run

```sh
gcloud run deploy {service_name} \
  --project={project} \
  --image={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/server \
  --platform=managed \
  --region={project} \
  --execution-environment=gen2 \
  --port=8080 \
  --no-allow-unauthenticated
```

## Deploy Portable Runner Pipeline (for Flink, Spark, ..etc)

```sh
mvn clean package -DskipTests -Pportable -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/portable:latest
```

## Deploy Flink Runner Pipeline

```sh
mvn clean package -DskipTests -Pflink -Dimage={region}-docker.pkg.dev/{deploy_project}/{template_repo_name}/flink:latest
```

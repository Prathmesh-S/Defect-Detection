# Submission Guide
This document was created by Owen Mariani to act as a reference down the line for all team members interested in submitting a solution to our team in the contest.

## General Resources 

🔗 [DEBS 2025 Challenger Website](https://challenge2025.debs.org/): Go here to do everything 

## Steps 
Note that only one job can be run at once

1. Containerize your solution (`docker build -t <name>`)
2. Create a Kubernetes Job 
3. Login to the [DEBS 2025 Challenger Website](https://challenge2025.debs.org/)
4. Upload the YAML file of your Kubernetes Job to `https://challenge2025.debs.org/deployment`
5. Click the Deploy Button

## Testing Docker Container Locally
You need to run the following commands in order for the docker container to run properly after you edit it:

```bash
# Build a new container
docker build -t python-example-solution .

# Run it locally using .env secrets
docker run --env-file .env python-example-solution
```

## Adding Docker Container to DockerHub
In order to create Kubernetes Job with Docker container, need to add to DockerHub

```bash
# Login to DockerHub
docker login

# Add `latest` tag
docker tag python-example-solution omariani24/python-example-solution:latest

# Push image to DockerHub
docker push omariani24/python-example-solution:latest

```

## Creating the Kubernetes Job
Follow these steps once you have already updated the Docker container on DockerHub
```bash
# Add the environment variable for API_KEY (if you haven't already)
kubectl create secret generic api-secret --from-env-file=.env

# Create Job from `job.yaml`
kubectl apply -f job.yaml

# Verify it worked 
kubectl get jobs
kubectl get pods

# See the output logs
kubectl logs -f <pod_name>
```
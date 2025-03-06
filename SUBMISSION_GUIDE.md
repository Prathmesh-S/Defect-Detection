# Submission Guide
This document was created by Owen Mariani to act as a reference down the line for all team members interested in submitting a solution to our team in the contest.

## General Resources 

🔗 [DEBS 2025 Challenger Website](https://challenge2025.debs.org/): Go here to do everything 

## Steps 
Note that only one job can be run at once

1. Containerize your solution (`docker build -t <name>`)
2. Create a Kubernetes Job 
    - Create a cluster using `kind`: `kind create cluster --name <name>`
    - Add Docker container to cluster using url of Docker Container on DockerHub: `kubectl apply -f <url>`
3. Login to the [DEBS 2025 Challenger Website](https://challenge2025.debs.org/)
4. Upload the YAML file of your Kubernetes Job to `https://challenge2025.debs.org/deployment`
5. Click the Deploy Button

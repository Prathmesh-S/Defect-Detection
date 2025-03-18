# Add the environment variable for API_KEY (if you haven't already)
# kubectl create secret generic api-secret --from-env-file=.env

# Delete Job if it already exists
# kubectl delete job <job_name>

# Make namespace if not already exists
kubectl create namespace group-4

# Create Job from `job.yaml`
# kubectl apply -f ../job.yaml

# Create Job for deployment `SUBMIT_TO_EVAL_job.yaml`
kubectl apply -f ../SUBMIT_TO_EVAL_job.yaml

# Verify it worked 
kubectl get jobs
kubectl get pods

# See the output logs
# kubectl logs -f <pod_name>
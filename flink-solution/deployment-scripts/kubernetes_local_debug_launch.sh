#!/bin/bash
# create new pods
kubectl apply -f ../complex-job.yaml

# get all pods to delete from
sleep 2
kubectl get pods --namespace group-4

# get logs for a pod
#kubectl logs -f <name_here> --namespace group-4

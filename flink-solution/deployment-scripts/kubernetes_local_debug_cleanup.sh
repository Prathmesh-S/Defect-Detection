  #!/bin/bash

  NAMESPACE="group-4"

  echo "Fetching and deleting Deployments..."
  deployments=$(kubectl get deployments --namespace "$NAMESPACE" --no-headers -o custom-columns=":metadata.name")
  if [ -n "$deployments" ]; then
      kubectl delete deployment $deployments --namespace "$NAMESPACE"
  else
      echo "No deployments found in namespace $NAMESPACE."
  fi

  echo "Fetching and deleting Jobs..."
  jobs=$(kubectl get jobs --namespace "$NAMESPACE" --no-headers -o custom-columns=":metadata.name")
  if [ -n "$jobs" ]; then
      kubectl delete job $jobs --namespace "$NAMESPACE"
  else
      echo "No jobs found in namespace $NAMESPACE."
  fi

  echo "Fetching and deleting Pods..."
  pods=$(kubectl get pods --namespace "$NAMESPACE" --no-headers -o custom-columns=":metadata.name")
  if [ -n "$pods" ]; then
      kubectl delete pod $pods --namespace "$NAMESPACE"
  else
      echo "No pods found in namespace $NAMESPACE."
  fi




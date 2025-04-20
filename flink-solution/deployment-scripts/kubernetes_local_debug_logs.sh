#!/bin/bash

# Namespace
NAMESPACE="group-4"

# Get pod names from the namespace
pods=$(kubectl get pods --namespace "$NAMESPACE" --no-headers | awk '{print $1}')

# Launch logs in new terminal for each pod
for pod in $pods; do
  osascript <<EOF
tell application "Terminal"
  do script "echo 'Showing logs for $pod...'; kubectl logs -f $pod --namespace $NAMESPACE"
end tell
EOF
done

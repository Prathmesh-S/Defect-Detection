# Launching Local Cluster with Kubernetes (Kind and Kubectl)

## Prerequisites 
Ensure that you meet the requirements by checking you have the following:

#### Kind
You should get a success message telling you the version if you have it
```
kind version
```
#### Kubectl
You should get a success message telling you the version if you have it
```
kubectl version
```

## Using the Scripts to Launch, Log, and Cleanup
The following scripts have been made to streamline the debugging process but are still very helpful for seeing the results locally!

Make sure you give them the permission to execute using `chmod +x <file_name>` for all scripts in this directory
1. Start the Cluster in namespace "group-4"
`kubernetes_local_debug_launch.sh`
2. Log the outputs of each pod launched (this will open up terminals in a separate window)
`kubernetes_local_debug_logs.sh`
3. Cleanup all that you've made when you're done
`kubernetes_local_debug_cleanup.sh`
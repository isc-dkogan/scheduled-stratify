#!/bin/sh

# Arguments
NAMESPACE=$1
SECRET_FILE=$2
APP_NAME=$3
CHART_PATH=$4
SERVICE_PORT=$5

echo "Installing $APP_NAME into $NAMESPACE from $CHART_PATH on port $SERVICE_PORT"

# Step 1 Delete namespace 
kubectl delete namespace $NAMESPACE 

# Step 2 Create the new namespace
kubectl create namespace $NAMESPACE

# Step 3 - This is needed when not on an SDS cluster
#kubectl create -f $SECRET_FILE

# Step 3 Copy secret from sds-cp into new namespace
kubectl get secret intersystems-container-registry-secret -n sds-cp -o yaml | sed 's/namespace: .*/namespace: "'"$NAMESPACE"'"/' | kubectl apply -f -

# Step 4 Install chart from CHART_PATH, can be dir or repo, refer to helm docs
helm install -n $NAMESPACE $APP_NAME  $CHART_PATH 

# Get the name and port for the new pod to do port forwarding to localhost
#POD_NAME=$(kubectl get pods -n $NAMESPACE  -l "app.kubernetes.io/name=stratify,app.kubernetes.io/instance=stratify" -o jsonpath="{.items[0].metadata.name}")
#CONTAINER_PORT=$(kubectl get pod -n $NAMESPACE  $POD_NAME -o jsonpath="{.spec.containers[0].ports[0].containerPort}")

#echo $POD_NAME
#echo $CONTAINER_PORT 

# This is only needed for localhost deployments
# read -p "pausing to let pod start before port forwarding" -t 10
# Step 5 forward the port so it can be accessed on localhost 
#kubectl --namespace $NAMESPACE  port-forward $POD_NAME $SERVICE_PORT:$CONTAINER_PORT

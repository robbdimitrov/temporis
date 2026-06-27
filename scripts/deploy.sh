#!/bin/bash
set -e

echo "Deploying Temporis..."

echo "1. Building and pushing Docker image to local registry..."
cd src && docker build -t localhost:5000/temporis:latest . && cd ..
docker push localhost:5000/temporis:latest

echo "2. Generating Postgres Init ConfigMap..."
kubectl create configmap postgres-init-script --from-file=pkg/database/schema.sql -o yaml --dry-run=client | kubectl apply -f -

echo "3. Applying Kubernetes manifests..."
kubectl apply -f deploy/

echo "4. Restarting Temporis deployment..."
kubectl rollout restart deployment temporis

echo "Deployment complete."

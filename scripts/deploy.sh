#!/bin/bash
set -e

NS="temporis"

echo "Deploying Temporis..."

echo "1. Building Docker image..."
cd src && docker build -t temporis:latest . && cd ..

echo "2. Ensuring namespace exists..."
kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

echo "3. Generating Postgres Init ConfigMap..."
kubectl create configmap postgres-init-script --from-file=pkg/database/schema.sql -o yaml --dry-run=client -n "${NS}" | kubectl apply -n "${NS}" -f -

echo "4. Applying Kubernetes manifests..."
kubectl apply -f deploy/ -n "${NS}"

echo "5. Restarting Temporis deployment..."
kubectl rollout restart deployment temporis -n "${NS}"

echo "Deployment complete."

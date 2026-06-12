#!/bin/bash
set -e

echo "Deploying Temporis..."

echo "1. Building Docker image..."
cd src && docker build -t temporis:1.0.1 . && cd ..

echo "2. Generating Postgres Init ConfigMap..."
kubectl create configmap postgres-init-script --from-file=pkg/database/script.sql -o yaml --dry-run=client | kubectl apply -f -

echo "3. Applying Kubernetes manifests..."
kubectl apply -f deploy/

echo "4. Restarting Temporis deployment (if exists)..."
kubectl rollout restart deployment temporis || true

echo "Deployment complete."

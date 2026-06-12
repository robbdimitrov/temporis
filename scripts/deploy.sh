#!/bin/bash
set -e

echo "Deploying Temporis..."

echo "1. Building Docker image..."
cd src && docker build -t temporis:1.0.0 . && cd ..

echo "2. Applying Kubernetes manifests..."
kubectl apply -f deploy/

echo "3. Restarting Temporis deployment (if exists)..."
kubectl rollout restart deployment temporis || true

echo "Deployment complete."

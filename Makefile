.PHONY: up down status

up:
	@echo "Starting up infrastructure..."
	docker-compose up -d
	@echo "Checking for Kubernetes cluster..."
	@kubectl cluster-info >/dev/null 2>&1 || (echo "Error: No Kubernetes cluster (e.g., Minikube/Docker Desktop) is running!" && exit 1)
	@echo "Applying Kubernetes manifests..."
	kubectl apply -f kubernetes/ 2>/dev/null || echo "No kubernetes folder found, skipping kubectl."
	@echo "Infrastructure is up!"

down:
	@echo "Tearing down infrastructure..."
	kubectl delete -f kubernetes/ 2>/dev/null || true
	docker-compose down
	@echo "Infrastructure is down."

status:
	docker-compose ps
	kubectl get pods 2>/dev/null || true


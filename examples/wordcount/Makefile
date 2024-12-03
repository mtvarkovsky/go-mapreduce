.PHONY: docker-build-wordcount
docker-build-wordcount: ## build wordcount docker image
	DOCKER_BUILDKIT=1 docker build --ssh default . -t wordcount:latest

.PHONY: docker-run-wodcount
docker-run-wordcount: ## runs wordcount docker image
	docker compose up
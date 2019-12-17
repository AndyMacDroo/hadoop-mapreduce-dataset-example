CONTAINER_NAME=nodemanager

build-job:
	mvn clean package

hadoop-cluster:
	docker-compose down
	docker-compose up -d

run-map-reduce: build-job hadoop-cluster
	docker exec -it ${CONTAINER_NAME} /bin/bash "/opt/scripts/exec-hadoop-job.sh"

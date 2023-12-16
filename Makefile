.PHONY: spark/network
spark/network:
	docker network create spark-network

.PHONY: spark/master
spark/master:
	docker run --rm -it --name spark-master --network spark-network -p 8080:8080 -p 7077:7077 bitnami/spark:latest

.PHONY: spark/worker/1
spark/worker/1:
	docker run --rm -it --name spark-worker-1 --network spark-network bitnami/spark:latest spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077

.PHONY: spark/worker/2
spark/worker/2:
	docker run --rm -it --name spark-worker-2 --network spark-network bitnami/spark:latest spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077

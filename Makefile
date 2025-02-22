build_all:
	docker build -t hadoop-hive-spark-base ./base
	docker build -t hadoop-hive-spark-master ./master
	docker build -t hadoop-hive-spark-worker ./worker
	docker build -t hadoop-hive-spark-history ./history
	docker build -t hadoop-hive-spark-jupyter ./jupyter
	# docker build -t hadoop-hive-spark-dev ./dev

build:
	docker build -t hadoop-hive-spark-base ./base
	docker build -t hadoop-hive-spark-master ./master
	docker build -t hadoop-hive-spark-worker ./worker
	docker build -t hadoop-hive-spark-history ./history

buildjupyter:
	docker build -t hadoop-hive-spark-jupyter ./jupyter

up:
	docker-compose up -d

down:
	docker-compose down
	
restart:
	docker-compose restart
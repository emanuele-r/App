postgres:
	psql  -p 5433 -c  "CREATE DATABASE asset_prices";

bash:

	./ps_nq.sh
	./ps_cs.sh
docker:
	docker build -t app .
	echo "Image Created"
	docker run -d --net=host  app:latest  

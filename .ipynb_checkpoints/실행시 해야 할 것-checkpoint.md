docker-compose up -d 
sh run_jupyter.sh
docker exec -it {container id} /bin/bash : worker 컨테이너에 들어간다 
apt-get update
apt-get install wget
pip install pandas
pip install biopython
pip install pysqlite3
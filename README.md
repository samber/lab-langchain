
# 🦜 Langchain: lab

https://github.com/samber/lab-langchain-getting-started

## IMDB

```sh
docker-compose up -d

# Fetch download link on the following page:
# https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/2QYZBT

wget -O dump_pg11 '<s3-download-url>'

apt install postgresql-client postgresql-client-common libpq-dev
pg_restore -d screeb -U screeb -h localhost -p 5432 --clean --if-exists -v dump_pg11
```

```sh
psql postgres://screeb:screeb@localhost:5432/screeb
```

## ENTSO-E

```sh
docker-compose up -d

export ES_ENDPOINT=http://elastic:screeb@localhost:9200

curl -s ${ES_ENDPOINT}/entsoe \
     -X PUT \
     -H 'Content-Type: application/json' \
     -d @index.json

curl -s ${ES_ENDPOINT}/entsoe/_mapping \
     -X PUT \
     -H 'Content-Type: application/json' \
     -d @mapping.json

go run entsoe-importer.go

pip install -r requirements.txt
python elasticsearch_main.py
```

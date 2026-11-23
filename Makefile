.PHONY: install data ingest transform quality all test clean

install:
	pip install -r requirements.txt

data:
	python -m pipeline.run generate --days 30

ingest:
	python -m pipeline.run ingest

transform:
	python -m pipeline.run transform

quality:
	python -m pipeline.run quality

all:
	python -m pipeline.run all --days 30

test:
	python -m pytest -q

clean:
	rm -rf data dbt/target dbt/logs logs

.PHONY: clean
# Remove all processing artifacts, build files and cache files
clean:
	rm -f poetry.lock
	rm -rf .ruff_cache/ .pytest_cache/
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type d -name '.data' -exec rm -rf {} +

.PHONY: lint
# Verify proper formatting for Python files
lint:
	poetry run ruff check .

.PHONY: format
# Automatic fix linting erros for all Python files
format:
	poetry run ruff check --fix .

.PHONY: test
# Run all project test suites
test:
	poetry run pytest tests/

.PHONY: crawl
# Run a webcrawler in background with default configs
# To record logs to the file set filename in `SCRAPY_LOG_TARGET` env variable
crawl:
	@if [ -n "$$SCRAPY_LOG_TARGET" ]; then \
		LOG_TARGET=$$SCRAPY_LOG_TARGET; \
	else \
		LOG_TARGET="/dev/null"; \
	fi; \
	cd src/webcrawlers && \
		nohup poetry run scrapy crawl gameswithreviews -o "../.data/raw.jl" > "$$LOG_TARGET" 2>&1 &

.PHONY: run-pipeline-repartition
# Run script for webcrawled data repartition
run-pipeline-repartition:
	cd src/pipeline; poetry run python -m repartition

.PHONY: run-pipeline-split
# Run script for split repartitioned dataset into separate entities
run-pipeline-split:
	cd src/pipeline; poetry run python -m split

.PHONY: etl
# Run ETL pipeline for webcrawled data
etl: run-pipeline-repartition run-pipeline-split


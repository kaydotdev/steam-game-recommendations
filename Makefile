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
crawl:
	cd src/webcrawlers; \
		nohup poetry run scrapy crawl gameswithreviews -o "../.data/raw.json" > /dev/null 2>&1 &


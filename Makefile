.PHONY: clean
# Remove all processing artifacts, build files and cache files.
clean: clean-data
	rm -rf .ruff_cache/ .pytest_cache/
	find . -type f -name '*.zip' -exec rm {} +
	find . -type d -name '__pycache__' -exec rm -rf {} +

.PHONY: clean-data
# Remove all crawled data.
clean-data:
	find . -type d -name '.data' -exec rm -rf {} +

.PHONY: ci
# Full continuous integration pipeline.
ci: lint test

.PHONY: lint
# Verify formatting for Python files.
lint:
	black --diff --check src/ tests/ webcrawl/ -q
	ruff check .

.PHONY: format
# Fix linting errors for all Python files.
format:
	black src/ tests/ webcrawl/ -q
	ruff check --fix .

.PHONY: test
# Run all project test suites.
test:
	pytest tests/

.PHONY: crawl
# Run web crawlers in background with default configs. Use the following command
# to run crawlers in the virtual environment. Can be used with SSH connection to
# a remote server. To record logs to the file set filename in 'SCRAPY_LOG_TARGET'
# env variable.
crawl:
	@if [ -n "$$SCRAPY_LOG_TARGET" ]; then \
		LOG_TARGET=$$SCRAPY_LOG_TARGET; \
	else \
		LOG_TARGET="/dev/null"; \
	fi; \
	cd webcrawl/ && \
		nohup scrapy crawl games -o "../.data/games.jl" > "$$LOG_TARGET" 2>&1 & && \
		nohup scrapy crawl reviews -o "../.data/reviews.jl" > "$$LOG_TARGET" 2>&1 &

.PHONY: zip-crawler
# Compresses web crawler scripts into a '*.zip' file for execution in a virtual environment.
zip-crawler:
	@which zip >/dev/null || (echo "'zip' utility not found" && exit 1)
	zip -r -9  webcrawlers.zip webcrawl/ -x "steampowered/__pycache__/*" -x "steampowered/spiders/__pycache__/*"

.PHONY: requirements
# Write or update all Poetry packages into separate `requirements.txt` files for each environment.
requirements:
	poetry export -f requirements.txt --only webcrawl --without-hashes --without-urls --output webcrawl/requirements.txt

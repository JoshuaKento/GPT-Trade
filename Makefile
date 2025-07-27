# Makefile for EDGAR Tools development
# Provides convenient commands for common development tasks

.PHONY: help install install-dev test test-cov lint format type-check security clean build upload docs serve-docs

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Installation targets
install: ## Install package for production use
	pip install .

install-dev: ## Install package with development dependencies
	pip install -e ".[dev,test,docs]"
	pre-commit install

# Testing targets
test: ## Run tests
	pytest

test-cov: ## Run tests with coverage report
	pytest --cov=edgar --cov-report=html --cov-report=term-missing

test-fast: ## Run tests excluding slow ones
	pytest -m "not slow"

test-integration: ## Run only integration tests
	pytest -m integration

test-unit: ## Run only unit tests
	pytest -m unit

test-parallel: ## Run tests in parallel
	pytest -n auto

# Code quality targets
lint: ## Run all linting tools
	flake8 .
	mypy edgar/ scripts/
	bandit -r edgar/ scripts/

format: ## Format code with black and isort
	black .
	isort .

format-check: ## Check if code is properly formatted
	black --check .
	isort --check-only .

type-check: ## Run type checking with mypy
	mypy edgar/ scripts/ --ignore-missing-imports

security: ## Run security checks
	bandit -r edgar/ scripts/
	safety check
	pip-audit

pre-commit: ## Run all pre-commit hooks
	pre-commit run --all-files

# Quality assurance - run everything
qa: format lint type-check security test ## Run all quality assurance checks

# Cleanup targets
clean: ## Clean up build artifacts and cache files
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .tox/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

clean-logs: ## Clean up log files
	find . -name "*.log" -delete
	rm -rf logs/

# Build and distribution targets
build: clean ## Build package distributions
	python -m build

build-check: build ## Build and check package
	twine check dist/*

upload-test: build-check ## Upload to Test PyPI
	twine upload --repository testpypi dist/*

upload: build-check ## Upload to PyPI
	twine upload dist/*

# Development targets
dev-setup: ## Set up development environment from scratch
	python -m venv venv
	@echo "Activate virtual environment with:"
	@echo "  Windows: venv\\Scripts\\activate"
	@echo "  macOS/Linux: source venv/bin/activate"
	@echo "Then run: make install-dev"

update-deps: ## Update all dependencies
	pip install --upgrade pip
	pip install --upgrade -e ".[dev,test,docs]"
	pre-commit autoupdate

# Documentation targets
docs: ## Build documentation
	cd docs && make html

docs-serve: docs ## Build and serve documentation locally
	cd docs/_build/html && python -m http.server 8000

docs-clean: ## Clean documentation build
	cd docs && make clean

# Utility targets
check-deps: ## Check for dependency issues
	pip check
	safety check

list-outdated: ## List outdated packages
	pip list --outdated

profile: ## Run profiling on main modules
	python -m cProfile -o profile.stats -m scripts.fetch_10k_new --help
	python -c "import pstats; pstats.Stats('profile.stats').sort_stats('cumulative').print_stats(20)"

# Docker targets (if using Docker)
docker-build: ## Build Docker image
	docker build -t edgar-tools .

docker-run: ## Run Docker container
	docker run --rm -it edgar-tools

docker-test: ## Run tests in Docker
	docker run --rm edgar-tools pytest

# Git helpers
git-clean: ## Clean up git repository
	git clean -fd
	git gc --prune=now

release-patch: ## Create a patch release
	@echo "Current version: $$(grep '^version = ' pyproject.toml | cut -d'"' -f2)"
	@read -p "Enter new patch version (x.y.z): " version; \
	sed -i 's/^version = .*/version = "'$$version'"/' pyproject.toml; \
	git add pyproject.toml; \
	git commit -m "Bump version to $$version"; \
	git tag -a v$$version -m "Release v$$version"; \
	echo "Created tag v$$version. Push with: git push origin v$$version"

# Environment info
info: ## Show environment information
	@echo "Python version: $$(python --version)"
	@echo "Pip version: $$(pip --version)"
	@echo "Virtual environment: $$(which python)"
	@echo "Project directory: $$(pwd)"
	@echo "Git branch: $$(git branch --show-current 2>/dev/null || echo 'Not in git repo')"
	@echo "Git status: $$(git status --porcelain 2>/dev/null | wc -l) modified files"

# Performance and monitoring
benchmark: ## Run performance benchmarks
	python -m pytest tests/ -k benchmark --benchmark-only

monitor: ## Run the EDGAR monitor script
	python -m scripts.monitor_new

# Database and data management (if applicable)
reset-data: ## Clear all downloaded data
	rm -rf data/
	rm -rf manifests/*.json
	@echo "Data cleared. Run fetch commands to repopulate."

# CI/CD helpers
ci-install: ## Install dependencies for CI
	pip install --upgrade pip
	pip install -e ".[dev,test]"

ci-test: ## Run tests in CI mode
	pytest --cov=edgar --cov-report=xml --junitxml=pytest.xml -v

ci-security: ## Run security checks in CI mode
	bandit -r edgar/ scripts/ -f json -o bandit-report.json
	safety check --json --output safety-report.json
	pip-audit --format=json --output=pip-audit-report.json
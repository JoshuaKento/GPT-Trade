# Development Setup Guide

This guide will help you set up a local development environment for the EDGAR Tools project.

## Prerequisites

- Python 3.9 or higher
- Git
- AWS credentials (for S3 operations)

## Quick Setup

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd GPT-Trade
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   # On Windows
   venv\Scripts\activate
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install development dependencies:**
   ```bash
   pip install -e ".[dev,test]"
   ```

4. **Install pre-commit hooks:**
   ```bash
   pre-commit install
   ```

5. **Run tests to verify setup:**
   ```bash
   pytest
   ```

## Development Workflow

### Code Quality

This project uses several tools to maintain code quality:

- **Black**: Code formatting
- **isort**: Import sorting
- **flake8**: Linting with plugins for docstrings, bugbear, comprehensions, and simplify
- **mypy**: Static type checking
- **bandit**: Security analysis
- **pre-commit**: Automated checks before commits

### Running Quality Checks

```bash
# Format code
black .
isort .

# Lint code
flake8 .

# Type checking
mypy edgar/ scripts/

# Security scan
bandit -r edgar/ scripts/

# Run all pre-commit hooks
pre-commit run --all-files
```

### Testing

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=edgar --cov-report=html

# Run specific test categories
pytest -m "not slow"  # Skip slow tests
pytest -m integration  # Only integration tests
pytest -m unit  # Only unit tests

# Run tests in parallel
pytest -n auto
```

### Adding Dependencies

1. **Production dependencies**: Add to `dependencies` in `pyproject.toml`
2. **Development dependencies**: Add to `dev` optional dependencies
3. **Test dependencies**: Add to `test` optional dependencies

After adding dependencies, update your environment:
```bash
pip install -e ".[dev,test]"
```

### Pre-commit Hooks

Pre-commit hooks run automatically before each commit. They include:

- Trailing whitespace removal
- End-of-file fixing
- YAML/TOML/JSON validation
- Python code formatting (black)
- Import sorting (isort)
- Linting (flake8)
- Type checking (mypy)
- Security scanning (bandit)
- Code modernization (pyupgrade)

To bypass hooks (not recommended):
```bash
git commit --no-verify
```

### Environment Configuration

#### AWS Configuration

For S3 operations, configure AWS credentials:

1. **Using AWS CLI:**
   ```bash
   aws configure
   ```

2. **Using environment variables:**
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

3. **Using IAM roles** (recommended for EC2/Lambda)

#### Project Configuration

Copy and modify configuration files:
```bash
cp config/apple_config.json config/my_config.json
# Edit config/my_config.json with your settings
```

## CI/CD Pipeline

### GitHub Actions Workflow

The project uses GitHub Actions for continuous integration:

- **Test Matrix**: Tests run on Ubuntu, Windows, and macOS with Python 3.9-3.12
- **Code Quality**: Automated linting, formatting checks, and type checking
- **Security**: Dependency vulnerability scanning
- **Coverage**: Code coverage reporting to Codecov
- **Build**: Package building and validation

### Automated Dependency Updates

Dependabot automatically creates PRs for:
- Python package updates (weekly)
- GitHub Actions updates (weekly)
- Grouped updates by category (testing, linting, AWS dependencies)

## Project Structure

```
GPT-Trade/
├── .github/
│   ├── workflows/
│   │   └── test.yml          # CI/CD pipeline
│   └── dependabot.yml        # Dependency update config
├── edgar/                    # Main package
│   ├── __init__.py
│   ├── client_new.py        # EDGAR API client
│   ├── companies.py         # Company data handling
│   ├── config_manager.py    # Configuration management
│   ├── filing_processor.py  # Filing processing logic
│   ├── filings.py          # Filing data structures
│   ├── logging_config.py    # Logging setup
│   ├── parser.py           # HTML/XML parsing
│   ├── progress.py         # Progress tracking
│   ├── s3_manager.py       # AWS S3 operations
│   ├── state.py            # State management
│   └── urls.py             # URL handling
├── scripts/                 # Command-line scripts
│   ├── companies.py
│   ├── fetch_10k.py
│   ├── fetch_10k_new.py
│   ├── list_files.py
│   ├── list_files_new.py
│   └── monitor_new.py
├── tests/                   # Test suite
│   ├── test_parser.py
│   ├── test_refactored_components.py
│   └── test_state.py
├── config/                  # Configuration files
├── manifests/              # Data manifests
├── .pre-commit-config.yaml # Pre-commit hooks
├── pyproject.toml          # Project configuration
├── requirements.txt        # Legacy requirements
└── DEVELOPMENT.md          # This file
```

## Common Development Tasks

### Adding a New Feature

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Implement your feature with tests
3. Run quality checks:
   ```bash
   pre-commit run --all-files
   pytest
   ```

4. Commit and push:
   ```bash
   git add .
   git commit -m "Add your feature description"
   git push -u origin feature/your-feature-name
   ```

5. Create a pull request

### Debugging

1. **Enable debug logging:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

2. **Use pytest debugging:**
   ```bash
   pytest --pdb  # Drop into debugger on failure
   pytest -s     # Don't capture output
   ```

3. **Profile performance:**
   ```bash
   python -m cProfile -o profile.stats your_script.py
   python -c "import pstats; pstats.Stats('profile.stats').sort_stats('cumulative').print_stats(20)"
   ```

### Release Process

1. Update version in `pyproject.toml`
2. Update changelog/release notes
3. Create and push tag:
   ```bash
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

4. GitHub Actions will automatically build and upload to PyPI (when configured)

## Troubleshooting

### Common Issues

1. **Import errors**: Ensure you installed the package in development mode (`pip install -e .`)
2. **Pre-commit failures**: Run `pre-commit run --all-files` to see specific issues
3. **Test failures**: Check AWS credentials and network connectivity
4. **Type checking errors**: Install missing type stubs or add `# type: ignore` comments

### Getting Help

- Check existing issues on GitHub
- Review the CI/CD logs for build failures
- Run tests locally to reproduce issues
- Use `pytest -v` for detailed test output

## Performance Considerations

- Use `aiohttp` for concurrent HTTP requests
- Implement caching for frequently accessed data
- Use S3 multipart uploads for large files
- Monitor memory usage with large datasets
- Profile critical code paths

## Security Guidelines

- Never commit sensitive data (API keys, passwords)
- Use environment variables for configuration
- Regularly update dependencies
- Run security scans with bandit
- Follow least-privilege principle for AWS permissions
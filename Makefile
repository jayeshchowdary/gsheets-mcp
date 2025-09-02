# Google Sheets MCP Server Makefile

.PHONY: help install auth start test clean setup

help: ## Show this help message
	@echo "Google Sheets MCP Server - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies with UV
	uv sync

setup: ## Run setup script to check prerequisites and guide setup
	uv run setup.py

auth: ## Run authentication script
	uv run authenticate.py

start: ## Start the MCP server
	uv run simplemcp.py

test: ## Test if server can import
	uv run python -c "import simplemcp; print('✅ Server ready!')"

test-import: ## Test if all modules can import
	uv run python -c "import simplemcp; print('✅ All imports successful')"

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .venv/

dev: ## Start server in development mode (with auto-reload)
	uv run simplemcp.py

check: ## Check project status
	@echo "🔍 Checking project status..."
	@echo "Python version: $(shell python --version)"
	@echo "UV version: $(shell uv --version)"
	@echo "Dependencies: $(shell uv pip list | wc -l) packages"
	@echo "Credentials: $(shell if [ -f credentials.json ]; then echo "✅ Found"; else echo "❌ Missing"; fi)"
	@echo "Token: $(shell if [ -f .token.json ]; then echo "✅ Found"; else echo "❌ Missing"; fi)"

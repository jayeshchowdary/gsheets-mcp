# Quick Start Guide

Get your Google Sheets MCP Server running in 5 minutes!

## 🚀 Super Quick Start

```bash
# 1. Clone and enter project
git clone https://github.com/jayeshchowdary/gsheets-mcp
cd gsheets-mcp

# 2. Install UV (if not installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# 3. Run setup
uv run setup.py

# 4. Authenticate (if needed)
uv run authenticate.py

# 5. Start server
uv run simplemcp.py
```

## 🔧 What Each Command Does

- **`uv run setup.py`** - Checks prerequisites, installs dependencies
- **`uv run authenticate.py`** - Sets up Google OAuth2 authentication
- **`uv run simplemcp.py`** - Starts the MCP server

## 📱 Test with Inspector

In another terminal:
```bash
npx @modelcontextprotocol/inspector
```

Connect to your server:
- **Command**: `uv`
- **Args**: `run simplemcp.py`
- **Working Directory**: `.` (current directory)

## 🆘 Need Help?

- **`make help`** - Show all available commands
- **`make check`** - Check project status
- **`make setup`** - Run full setup again

## 📁 Project Structure

```
gsheets-mcp/
├── simplemcp.py            # Main MCP server
├── authenticate.py          # Authentication script
├── setup.py                # Setup helper
├── pyproject.toml          # UV configuration
├── mcp-server-config.json  # MCP client config
├── Makefile                # Convenience commands
├── README.md               # Full documentation
└── .gitignore              # Git exclusions
```

## 🔑 Authentication Files

- **`credentials.json`** - Download from Google Cloud Console
- **`.token.json`** - Generated after running `authenticate.py`

**Never commit these files to git!**

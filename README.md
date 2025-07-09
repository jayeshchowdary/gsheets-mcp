# OAuth MCP Demo

A demonstration of OAuth authentication with Google services using the Model Context Protocol (MCP) and FastMCP.

## Overview

This project showcases how to implement OAuth 2.0 authentication for Google services (For example youtube and drive access) within an MCP server. It provides tools for authenticating users and accessing Google APIs securely.

## Features

- 🔐 OAuth 2.0 authentication with Google
- 📺 YouTube API access (read-only)
- 📁 Google Drive API access
- 📄 Google Docs API access
- 🔧 MCP server implementation using FastMCP
- 💾 Token persistence for seamless re-authentication

## Prerequisites

- Python 3.8+
- Google Cloud Project with OAuth 2.0 credentials
- uv (Python package manager)

## Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd oauthmcp-demo
   ```

2. **Install dependencies:**
   ```bash
   uv sync
   ```

3. **Set up Google OAuth credentials:**
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the YouTube Data API v3, Google Drive API, and Google Docs API
   - Create OAuth 2.0 credentials (Desktop application)
   - Download the credentials

4. **Configure environment variables:**
   Create a `.env` file in the project root:
   ```
   GOOGLE_CLIENT_ID=your_client_id_here
   GOOGLE_CLIENT_SECRET=your_client_secret_here
   ```

## Usage

1. **Test the MCP server in the inspector:**
   ```bash
   npx @modelcontextprotocol/inspector uv --directory . run python simplemcp.py
   ```

2. **Authenticate with Google:**
   Use the `authenticate` tool to start the OAuth flow. This will:
   - Generate an authorization URL
   - Open your browser automatically (if possible)
   - Prompt you to copy the authorization code

3. **Complete authentication:**
   Use the `complete_auth` tool with the authorization code from step 2.

## Available Tools

### `authenticate()`
Initiates the OAuth 2.0 flow with Google. Returns an authorization URL that the user needs to visit.

### `complete_auth(authorization_code: str)`
Completes the OAuth flow using the authorization code from Google. Saves the credentials for future use.

## Security Notes

- The `.env` file containing your OAuth credentials is excluded from version control
- OAuth tokens are stored locally in `.token.json` (also excluded from git)
- The project uses the "out-of-band" OAuth flow suitable for desktop applications

## API Scopes

The application requests access to:
- `https://www.googleapis.com/auth/youtube.readonly` - Read-only YouTube access
- `https://www.googleapis.com/auth/drive.file` - Google Drive file access
- `https://www.googleapis.com/auth/documents` - Google Docs access

## Contributing

Feel free to submit issues and pull requests to improve this demo.

## License

This project is provided as-is for educational purposes.

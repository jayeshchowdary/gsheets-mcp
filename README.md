# Google Sheets MCP Server

A Model Context Protocol (MCP) server that provides tools for interacting with Google Sheets and Google Drive using OAuth2 authentication.

## Features

- **Authentication**: OAuth2-based authentication with Google APIs
- **Google Sheets**: Create, read, update, and delete spreadsheets and worksheets
- **Google Drive**: Search and manage files
- **Data Operations**: Batch operations, data filtering, and SQL-like queries
- **Formatting**: Cell formatting, charts, and conditional formatting

## Prerequisites

- Python 3.12+
- [UV package manager](https://docs.astral.sh/uv/getting-started/installation/)
- Google Cloud Project with Google Sheets API enabled
- OAuth2 credentials from Google Cloud Console

## Quick Start

### 1. Clone and Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd oauthmcp

# Install dependencies with UV
uv sync
```

### 2. Authentication Setup

1. **Download your `credentials.json`** from Google Cloud Console
2. **Place it in the project directory**
3. **Run authentication**:
   ```bash
   uv run authenticate.py
   ```
4. **Follow the prompts** to complete OAuth2 authentication

### 3. Start the MCP Server

```bash
# Start the server
uv run simplemcp.py
```

### 4. Test with MCP Inspector

```bash
# In another terminal, start the inspector
npx @modelcontextprotocol/inspector

# Connect to your server using the inspector interface
# Command: uv
# Args: run simplemcp.py
# Working Directory: . (current directory)
```

## Project Structure

```
oauthmcp/
├── pyproject.toml          # UV project configuration
├── uv.lock                 # Locked dependencies
├── simplemcp.py            # Main MCP server
├── authenticate.py         # Authentication script
├── credentials.json        # OAuth2 credentials (not in git)
├── .token.json            # Authentication token (not in git)
├── mcp-server-config.json # MCP client configuration
└── README.md              # This file
```

## Available Tools

### Spreadsheet Management
- `list_sheets` - List all Google Sheets in your Drive
- `create_google_sheet` - Create a new Google Sheet
- `delete_spreadsheet` - Delete a Google Sheet
- `get_spreadsheet_info` - Get spreadsheet metadata

### Worksheet Management
- `add_worksheet` - Add a new worksheet to a spreadsheet
- `delete_sheet` - Delete a worksheet from a spreadsheet
- `get_sheet_names` - Get all worksheet names
- `find_worksheet_by_title` - Find worksheet by exact title
- `copy_sheet_to_another_spreadsheet` - Copy sheet between spreadsheets

### Data Operations
- `get_cell_data` - Get data from specific cells
- `update_sheet_data` - Update cell data
- `append_values_to_spreadsheet` - Append data to spreadsheet
- `batch_update_by_filter` - Update values by data filter
- `execute_sql_query` - Execute SQL-like queries on sheets

### Data Filtering
- `get_spreadsheet_by_data_filter` - Get data using filters
- `batch_get_spreadsheet_values_by_data_filter` - Get values by data filter
- `batch_clear_spreadsheet_values` - Clear multiple ranges
- `batch_clear_values_by_data_filter` - Clear values by data filter

### Table Operations
- `list_tables` - List all tables in a spreadsheet
- `get_table_schema` - Get table structure and schema
- `create_sheet_from_json` - Create sheet from JSON data

### Formatting and Charts
- `format_cell` - Apply cell formatting
- `create_chart` - Create charts in sheets
- `set_basic_filter` - Set basic filters
- `clear_basic_filter` - Clear basic filters

### Dimension Management
- `append_dimension` - Add rows/columns
- `insert_dimension` - Insert rows/columns at specific positions
- `delete_dimension` - Delete rows/columns
- `create_spreadsheet_column` - Create new columns
- `create_spreadsheet_row` - Create new rows

### Developer Metadata
- `create_developer_metadata` - Create metadata
- `search_developer_metadata` - Search metadata
- `delete_developer_metadata` - Delete metadata

### Properties Management
- `update_sheet_properties` - Update worksheet properties
- `update_spreadsheet_properties` - Update spreadsheet properties

### Drive Operations
- `search_spreadsheets` - Search for spreadsheets in Drive

## Development

### Adding Dependencies

```bash
# Add new package
uv add package-name

# Sync after changes
uv sync
```

### Testing

```bash
# Test if server can import
uv run python -c "import simplemcp; print('✅ Ready!')"

# Test credentials
uv run python -c "import simplemcp; print(f'Credentials: {simplemcp.creds is not None}')"
```

## Troubleshooting

### Common Issues

1. **"Credentials file not found"**
   - Ensure `credentials.json` exists in project directory
   - Run `uv run authenticate.py` to set up authentication

2. **"Token file not found"**
   - Run `uv run authenticate.py` to create a new token

3. **"Module not found"**
   - Run `uv sync` to install dependencies

4. **"Authentication required"**
   - Run `uv run authenticate.py` to set up authentication

### UV-Specific Issues

1. **"Command not found: uv"**
   - Install UV: `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Restart your terminal

2. **"Failed to install dependencies"**
   - Clear UV cache: `uv cache clean`
   - Reinstall: `uv sync --reinstall`

## Security Notes

- **Never commit `credentials.json` or `.token.json`** to version control
- **Keep your OAuth2 credentials secure**
- **Tokens are automatically refreshed** when needed

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify UV is properly installed and dependencies are synced
3. Ensure Google Sheets API is enabled in your Google Cloud Project
4. Check that your OAuth2 credentials have the correct scopes

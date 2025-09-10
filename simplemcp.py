from pathlib import Path
import json
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
import webbrowser
from fastmcp import FastMCP
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.auth.transport.requests import Request

load_dotenv()


simple_mcp = FastMCP("google-sheets-mcp")


# Load credentials from environment variables passed by MCP client
import os
import sys

# Define scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

# Initialize credential variables
CLIENT_ID = None
CLIENT_SECRET = None
REDIRECT_URI = None
CREDENTIALS_FILE = None
TOKEN_FILE = None
creds = None

# For MCP server mode, suppress all credential errors to avoid breaking the protocol
# Only show errors when running standalone
SHOW_CREDENTIAL_ERRORS = False  # Always suppress for MCP compatibility

def load_credentials():
    """Load credentials from environment variables"""
    global CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, CREDENTIALS_FILE, TOKEN_FILE, creds
    
    # Get fresh environment variables (in case they were updated)
    current_creds_path = os.getenv('GSHEETS_CREDENTIALS_PATH')
    current_token_path = os.getenv('GSHEETS_TOKEN_PATH')
    

    
    # Load credentials if path is available
    if current_creds_path:
        CREDENTIALS_FILE = Path(current_creds_path)

        
        if CREDENTIALS_FILE.exists():
            try:
                with open(CREDENTIALS_FILE, 'r') as f:
                    client_config = json.load(f)
                
                # Handle both 'installed' and 'web' credential types
                if 'installed' in client_config:
                    cred_section = client_config['installed']
                elif 'web' in client_config:
                    cred_section = client_config['web']
                else:
                    raise ValueError("Credentials file must contain either 'installed' or 'web' section")
                
                CLIENT_ID = cred_section['client_id']
                CLIENT_SECRET = cred_section['client_secret']
                # For web credentials, use the first redirect URI
                REDIRECT_URI = cred_section['redirect_uris'][0]

            except Exception as e:

                if SHOW_CREDENTIAL_ERRORS:
                    sys.stderr.write(f"Error loading credentials from {current_creds_path}: {e}\n")
                CLIENT_ID = None
                CLIENT_SECRET = None
                REDIRECT_URI = None
        else:
            # Reset credentials if file doesn't exist
            CLIENT_ID = None
            CLIENT_SECRET = None
            REDIRECT_URI = None

    # Load token if path is available
    if current_token_path:
        TOKEN_FILE = Path(current_token_path)
        
        if TOKEN_FILE.exists():
            try:
                token_data = json.loads(TOKEN_FILE.read_text())
                creds = Credentials.from_authorized_user_info(token_data, SCOPES)
            except Exception as e:
                if SHOW_CREDENTIAL_ERRORS:
                    sys.stderr.write(f"Error loading token from {current_token_path}: {e}\n")
                    sys.stderr.write("Token file may be corrupted. Please re-authenticate.\n")
                creds = None
        else:
            creds = None


# Load credentials immediately at startup (like Gmail MCP server does)
load_credentials()

def get_auth_error_response():
    """Return a standardized authentication error response"""
    return {
        "successful": False,
        "message": "Google Sheets credentials not configured. Please set GSHEETS_CREDENTIALS_PATH and GSHEETS_TOKEN_PATH environment variables and authenticate.",
        "error": "Missing credentials configuration",
        "instructions": [
            "1. Download your credentials.json from Google Cloud Console",
            "2. Place it in your project directory",
            "3. Run the authenticate.py script to generate your token",
            "4. Ensure your MCP client configuration points to the correct paths"
        ]
    }

def check_credentials():
    """Check if credentials are available and valid"""
    global creds, CLIENT_ID, CLIENT_SECRET, REDIRECT_URI
    
    # Try to reload credentials from environment variables if not already loaded
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        load_credentials()
    
    # Check if credentials are available
    if not CLIENT_ID or not CLIENT_SECRET or not REDIRECT_URI:
        return False, "Missing credentials configuration"
    
    # Check if we have valid credentials
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                return True, None
            except Exception as e:
                return False, f"Token refresh failed: {str(e)}"
        else:
            return False, "Authentication required"
    
    return True, None


@simple_mcp.tool()
async def list_sheets(max_results: int = 50, page_token: str | None = None) -> dict:
    """List Google Sheets in your Google Drive with pagination support
    
    Args:
        max_results: Maximum number of sheets to return (1-1000). Defaults to 50.
        page_token: Token for the next page of results. Use this for pagination.
    """

    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    # Validate max_results parameter
    if max_results < 1 or max_results > 1000:
        return {
            "successful": False,
            "message": "Error: max_results must be between 1 and 1000",
            "error": "Invalid max_results value"
        }
    
    try:
        # Build the Drive service
        service = build('drive', 'v3', credentials=creds)
        
        # Prepare the request parameters
        request_params = {
            'q': "mimeType='application/vnd.google-apps.spreadsheet'",
            'pageSize': max_results,
            'fields': "nextPageToken, files(id, name, createdTime, modifiedTime, size, owners, shared, starred, trashed, webViewLink)"
        }
        
        # Add page token if provided
        if page_token:
            request_params['pageToken'] = page_token
        
        # Search for spreadsheet files
        results = service.files().list(**request_params).execute()
        
        files = results.get('files', [])
        next_page_token = results.get('nextPageToken')
        
        if not files:
            return {
                "successful": True,
                "message": "No Google Sheets found in your Drive",
                "sheets": [],
                "pagination": {
                    "has_more": False,
                    "next_page_token": None,
                    "total_estimated": 0
                },
                "summary": {
                    "returned_count": 0,
                    "max_results": max_results
                }
            }
        
        # Format the response
        sheets = []
        for file in files:
            sheet_info = {
                "name": file.get('name', ''),
                "id": file.get('id', ''),
                "created_time": file.get('createdTime', ''),
                "modified_time": file.get('modifiedTime', ''),
                "size": file.get('size', '0'),
                "shared": file.get('shared', False),
                "starred": file.get('starred', False),
                "trashed": file.get('trashed', False),
                "web_view_link": file.get('webViewLink', ''),
                "owners": [owner.get('displayName', '') for owner in file.get('owners', []) if isinstance(owner, dict)]
            }
            
            # Add human-readable dates
            if sheet_info['created_time']:
                try:
                    from datetime import datetime
                    created_dt = datetime.fromisoformat(sheet_info['created_time'].replace('Z', '+00:00'))
                    sheet_info['created_date'] = created_dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    sheet_info['created_date'] = sheet_info['created_time']
            
            if sheet_info['modified_time']:
                try:
                    from datetime import datetime
                    modified_dt = datetime.fromisoformat(sheet_info['modified_time'].replace('Z', '+00:00'))
                    sheet_info['modified_date'] = modified_dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    sheet_info['modified_date'] = sheet_info['modified_time']
            
            # Add size in human-readable format
            if sheet_info['size'] != '0':
                try:
                    size_bytes = int(sheet_info['size'])
                    if size_bytes < 1024:
                        sheet_info['size_formatted'] = f"{size_bytes} B"
                    elif size_bytes < 1024 * 1024:
                        sheet_info['size_formatted'] = f"{size_bytes / 1024:.1f} KB"
                    else:
                        sheet_info['size_formatted'] = f"{size_bytes / (1024 * 1024):.1f} MB"
                except:
                    sheet_info['size_formatted'] = sheet_info['size']
            else:
                sheet_info['size_formatted'] = "0 B"
            
            sheets.append(sheet_info)
        
        # Build pagination info
        pagination_info = {
            "has_more": next_page_token is not None,
            "next_page_token": next_page_token,
            "total_estimated": len(sheets) + (len(sheets) if next_page_token else 0)  # Rough estimate
        }
        
        # Build summary info
        summary_info = {
            "returned_count": len(sheets),
            "max_results": max_results,
            "page_number": 1 if not page_token else "N/A"  # Google doesn't provide page numbers
        }
        
        return {
            "successful": True,
            "message": f"Found {len(sheets)} Google Sheets (showing up to {max_results})",
            "sheets": sheets,
            "pagination": pagination_info,
            "summary": summary_info
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error listing sheets: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def add_worksheet(spreadsheet_id: str, title: str, row_count: int = 1000, column_count: int = 26) -> dict:
    """Add a new worksheet (tab) to an existing Google Sheet
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        title: The name of the new worksheet
        row_count: Number of rows in the new worksheet (default: 1000)
        column_count: Number of columns in the new worksheet (default: 26)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Prepare the request body for adding a new sheet
        request_body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': title,
                        'gridProperties': {
                            'rowCount': row_count,
                            'columnCount': column_count
                        }
                    }
                }
            }]
        }
        
        # Execute the request to add the new sheet
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()
        
        # Get the new sheet ID from the response
        new_sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
        
        return {
            "successful": True,
            "message": f"Successfully added new worksheet '{title}' to the spreadsheet",
            "sheet_id": new_sheet_id
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error adding worksheet: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def append_dimension(spreadsheet_id: str, sheet_id: int, dimension: str, length: int) -> dict:
    """Append new rows or columns to a sheet, increasing its size
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_id: The ID of the specific sheet/tab to modify (integer)
        dimension: Either 'ROWS' or 'COLUMNS' to specify what to append
        length: The number of rows or columns to append
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        return get_auth_error_response()
    
    # Validate dimension parameter
    if dimension.upper() not in ['ROWS', 'COLUMNS']:
        return {
            "successful": False,
            "message": "Error: dimension must be either 'ROWS' or 'COLUMNS'",
            "error": "Invalid dimension parameter"
        }
    
    if length <= 0:
        return {
            "successful": False,
            "message": "Error: length must be greater than 0",
            "error": "Invalid length parameter"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Prepare the request body for appending dimensions
        request_body = {
            'requests': [{
                'appendDimension': {
                    'sheetId': sheet_id,
                    'dimension': dimension.upper(),
                    'length': length
                }
            }]
        }
        
        # Execute the request to append dimensions
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=request_body
        ).execute()
        
        return {
            "successful": True,
            "message": f"Successfully appended {length} {dimension.lower()} to the sheet",
            "sheet_id": sheet_id,
            "dimension": dimension.upper(),
            "length": length
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error appending dimension: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def get_cell_data(spreadsheet_id: str, ranges: list[str]) -> dict:
    """Retrieve data from specified cell ranges in a Google Spreadsheet
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        ranges: List of cell ranges in A1 notation (e.g., ['Sheet1!A1:B2', 'A1:C5'])
                If no sheet name is specified, defaults to the first sheet
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        return get_auth_error_response()
    
    if not ranges or len(ranges) == 0:
        return {
            "successful": False,
            "message": "Error: At least one range must be specified",
            "error": "Missing range parameter"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return {
                "successful": False,
                "message": "Error: Spreadsheet has no worksheets",
                "error": "No worksheets found"
            }
        
        # Get available sheet names for validation
        available_sheets = {sheet['properties']['title'] for sheet in spreadsheet['sheets']}
        
        # Validate ranges and extract sheet names
        validated_ranges = []
        for range_str in ranges:
            if '!' in range_str:
                # Range includes sheet name
                sheet_name, cell_range = range_str.split('!', 1)
                if sheet_name not in available_sheets:
                    return {
                        "successful": False,
                        "message": f"Error: Sheet '{sheet_name}' not found",
                        "error": "Sheet not found",
                        "available_sheets": list(available_sheets)
                    }
                validated_ranges.append(range_str)
            else:
                # No sheet name specified, use first sheet
                first_sheet_name = spreadsheet['sheets'][0]['properties']['title']
                validated_ranges.append(f"{first_sheet_name}!{range_str}")
        
        # Retrieve data from all specified ranges
        response = service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=validated_ranges
        ).execute()
        
        # Format the response
        result_data = []
        
        for i, value_range in enumerate(response.get('valueRanges', [])):
            range_name = validated_ranges[i]
            values = value_range.get('values', [])
            
            result_data.append({
                "range": range_name,
                "values": values if values else []
            })
        
        return {
            "successful": True,
            "message": f"Successfully retrieved data from {len(result_data)} ranges",
            "data": result_data,
            "spreadsheet_id": spreadsheet_id
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error retrieving cell data: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def update_sheet_data(spreadsheet_id: str, values: list[list], sheet_name: str | None = None, first_cell_location: str | None = None) -> dict:
    """Update a specified range in a Google Sheet with given values, or append them as new rows if first_cell_location is omitted
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        values: 2D list of cell values. Each inner list represents a row
        sheet_name: Name of the specific sheet to update. If None, uses the first sheet
        first_cell_location: Starting cell for the update range in A1 notation (e.g., 'A1', 'B2'). 
                           If omitted, values are appended as new rows
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        return get_auth_error_response()
    
    if not values or len(values) == 0:
        return {
            "successful": False,
            "message": "Error: At least one row of values must be provided",
            "error": "Missing values parameter"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access.",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return {
                "successful": False,
                "message": "Error: Spreadsheet has no worksheets",
                "error": "No worksheets found"
            }
        
        # Determine which sheet to use
        target_sheet = None
        if sheet_name:
            # Find the specified sheet
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    target_sheet = sheet
                    break
            if not target_sheet:
                available_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
                return {
                    "successful": False,
                    "message": f"Error: Sheet '{sheet_name}' not found",
                    "error": "Sheet not found",
                    "available_sheets": available_sheets
                }
        else:
            # Use the first sheet
            target_sheet = spreadsheet['sheets'][0]
            sheet_name = target_sheet['properties']['title']
        
        # Prepare the range for the update
        if first_cell_location:
            # Update existing range - use the exact location provided
            range_name = f"{sheet_name}!{first_cell_location}"
        else:
            # Append as new rows - get the next available row
            try:
                # Get the current data to find the next empty row
                current_data = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!A:A"
                ).execute()
                
                current_values = current_data.get('values', [])
                next_row = len(current_values) + 1
                range_name = f"{sheet_name}!A{next_row}"
            except Exception as e:
                # If we can't determine the next row, start from A1
                range_name = f"{sheet_name}!A1"
        
        # Update the sheet with the new values
        body = {
            'values': values
        }
        
        response = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        
        updated_cells = response.get('updatedCells', 0)
        
        if first_cell_location:
            return {
                "successful": True,
                "message": f"Successfully updated {updated_cells} cells in range {range_name}",
                "updated_cells": updated_cells,
                "range": range_name,
                "sheet_name": sheet_name
            }
        else:
            return {
                "successful": True,
                "message": f"Successfully appended {len(values)} new rows to {sheet_name}, starting at {range_name}",
                "appended_rows": len(values),
                "sheet_name": sheet_name,
                "start_range": range_name
            }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error updating sheet data: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def batch_update_by_filter(spreadsheet_id: str, sheet_name: str, filter_column: str, filter_value: str, update_column: str, new_value: str) -> dict:
    """Tool to update values in ranges matching data filters. Use when you need to update specific data in a Google Sheet based on criteria rather than fixed cell ranges.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_name: Name of the specific sheet to update
        filter_column: Column letter (e.g., 'A', 'B') or column name to filter by
        filter_value: Value to match in the filter column
        update_column: Column letter (e.g., 'C', 'D') or column name to update
        new_value: New value to set in the update column for matching rows
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e),
                "updated_rows": 0,
                "matching_rows": []
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return {
                "successful": False,
                "message": "Error: Spreadsheet has no worksheets",
                "error": "No worksheets found",
                "updated_rows": 0,
                "matching_rows": []
            }
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == sheet_name:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "available_sheets": available_sheets,
                "updated_rows": 0,
                "matching_rows": []
            }
        
        # Get all data from the sheet
        try:
            data_response = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A:Z"  # Get data from A to Z columns
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not retrieve data from sheet. Details: {str(e)}",
                "error": str(e),
                "updated_rows": 0,
                "matching_rows": []
            }
        
        values = data_response.get('values', [])
        if not values:
            return {
                "successful": False,
                "message": "Error: Sheet is empty",
                "error": "No data found",
                "updated_rows": 0,
                "matching_rows": []
            }
        
        # Find header row and column indices
        headers = values[0]
        
        # Convert column letters to indices if needed
        def column_to_index(column_ref):
            if column_ref.isalpha():
                # Convert column letter to index (A=0, B=1, etc.)
                col = 0
                for char in column_ref.upper():
                    col = col * 26 + (ord(char) - ord('A') + 1)
                return col - 1
            else:
                # Assume it's already a column name
                try:
                    return headers.index(column_ref)
                except ValueError:
                    return None
        
        filter_col_idx = column_to_index(filter_column)
        update_col_idx = column_to_index(update_column)
        
        if filter_col_idx is None:
            return {
                "successful": False,
                "message": f"Error: Filter column '{filter_column}' not found. Available columns: {', '.join(headers)}",
                "error": "Filter column not found",
                "available_columns": headers,
                "updated_rows": 0,
                "matching_rows": []
            }
        
        if update_col_idx is None:
            return {
                "successful": False,
                "message": f"Error: Update column '{update_column}' not found. Available columns: {', '.join(headers)}",
                "error": "Update column not found",
                "available_columns": headers,
                "updated_rows": 0,
                "matching_rows": []
            }
        
        # Find rows that match the filter criteria
        matching_rows = []
        for row_idx, row in enumerate(values[1:], start=2):  # Start from row 2 (after header)
            if len(row) > filter_col_idx and str(row[filter_col_idx]) == filter_value:
                matching_rows.append(row_idx)
        
        if not matching_rows:
            return {
                "successful": False,
                "message": f"No rows found where column '{filter_column}' equals '{filter_value}'",
                "error": "No matching rows",
                "filter_column": filter_column,
                "filter_value": filter_value,
                "updated_rows": 0,
                "matching_rows": []
            }
        
        # Prepare batch update requests
        requests = []
        for row_idx in matching_rows:
            # Calculate the A1 notation for the cell to update
            col_letter = chr(ord('A') + update_col_idx)
            cell_range = f"{sheet_name}!{col_letter}{row_idx}"
            
            # Add update request
            requests.append({
                'updateCells': {
                    'range': {
                        'sheetId': target_sheet['properties']['sheetId'],
                        'startRowIndex': row_idx - 1,  # 0-based index
                        'endRowIndex': row_idx,
                        'startColumnIndex': update_col_idx,
                        'endColumnIndex': update_col_idx + 1
                    },
                    'rows': [{
                        'values': [{
                            'userEnteredValue': {
                                'stringValue': new_value
                            }
                        }]
                    }],
                    'fields': 'userEnteredValue'
                }
            })
        
        # Execute batch update
        batch_body = {
            'requests': requests
        }
        
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=batch_body
        ).execute()
        
        return {
            "successful": True,
            "message": f"Successfully updated {len(matching_rows)} rows where column '{filter_column}' equals '{filter_value}'. Updated column '{update_column}' with value '{new_value}'.",
            "updated_rows": len(matching_rows),
            "matching_rows": matching_rows,
            "filter_column": filter_column,
            "filter_value": filter_value,
            "update_column": update_column,
            "new_value": new_value
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error updating values by filter: {str(e)}",
            "error": str(e),
            "updated_rows": 0,
            "matching_rows": []
        }


@simple_mcp.tool()
async def clear_basic_filter(spreadsheet_id: str, sheet_name: str) -> dict:
    """Tool to clear the basic filter from a sheet. Use when you need to remove an existing basic filter from a specific sheet within a Google spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_name: Name of the specific sheet to clear the filter from
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e),
                "sheet_name": sheet_name
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return {
                "successful": False,
                "message": "Error: Spreadsheet has no worksheets",
                "error": "No worksheets found",
                "sheet_name": sheet_name
            }
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == sheet_name:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "available_sheets": available_sheets,
                "sheet_name": sheet_name
            }
        
        # Get the sheet ID
        sheet_id = target_sheet['properties']['sheetId']
        
        # Clear the basic filter from the sheet
        try:
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    'requests': [{
                        'clearBasicFilter': {
                            'sheetId': sheet_id
                        }
                    }]
                }
            ).execute()
            
            return {
                "successful": True,
                "message": f"Successfully cleared basic filter from sheet '{sheet_name}'",
                "sheet_name": sheet_name,
                "sheet_id": sheet_id
            }
            
        except Exception as e:
            # Check if the error is because there's no filter to clear
            if "no filter" in str(e).lower() or "filter not found" in str(e).lower():
                return {
                    "successful": False,
                    "message": f"No basic filter found on sheet '{sheet_name}' to clear",
                    "error": "No filter to clear",
                    "sheet_name": sheet_name,
                    "sheet_id": sheet_id
                }
            else:
                return {
                    "successful": False,
                    "message": f"Error clearing basic filter: {str(e)}",
                    "error": str(e),
                    "sheet_name": sheet_name,
                    "sheet_id": sheet_id
                }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error clearing basic filter: {str(e)}",
            "error": str(e),
            "sheet_name": sheet_name
        }


@simple_mcp.tool()
async def clear_spreadsheet_values(spreadsheet_id: str, range: str) -> dict:
    """Clears cell content (preserving formatting and notes) from a specified A1 notation range in a Google spreadsheet. The range must correspond to an existing sheet and cells.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        range: A1 notation range to clear (e.g., 'Sheet1!A1:B5', 'A1:C10', 'B2:D8')
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not range:
        return {
            "successful": False,
            "message": "Error: Range must be specified",
            "error": "Missing range parameter"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Parse the range to extract sheet name and cell range
        sheet_name = None
        cell_range = None
        
        if '!' in range:
            # Range includes sheet name (e.g., 'Sheet1!A1:B5')
            sheet_name, cell_range = range.split('!', 1)
            # Clean up any extra quotes or whitespace from both parts
            sheet_name = sheet_name.strip().strip("'\"")
            cell_range = cell_range.strip().strip("'\"")
        else:
            # No sheet name specified, use first sheet
            sheet_name = spreadsheet['sheets'][0]['properties']['title']
            cell_range = range.strip().strip("'\"")
        
        # Verify the sheet exists
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == sheet_name:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "available_sheets": available_sheets
            }
        
        # Construct the clean range for the API call
        clean_range = f"{sheet_name}!{cell_range}"
        
        # Clear the values from the specified range
        try:
            response = service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=clean_range,
                body={}
            ).execute()
            
            # Get the cleared range info
            cleared_range = response.get('clearedRange', clean_range)
            
            return {
                "successful": True,
                "message": f"Successfully cleared values from range '{cleared_range}'. Formatting and notes have been preserved.",
                "cleared_range": cleared_range
            }
            
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error clearing values from range '{clean_range}': {str(e)}",
                "error": str(e),
                "range": clean_range
            }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error clearing spreadsheet values: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def create_chart(spreadsheet_id: str, sheet_name: str, chart_title: str, data_range: str, chart_type: str = "COLUMN", position: str = "A1") -> dict:
    """Create a chart in a Google Sheets spreadsheet using the specified data range and chart type.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_name: Name of the sheet where the chart will be created
        chart_title: Title for the chart
        data_range: A1 notation range containing the data (e.g., 'A1:C10')
        chart_type: Type of chart to create (default: 'COLUMN')
                  Options: 'COLUMN', 'LINE', 'PIE', 'BAR', 'AREA', 'SCATTER', 'HISTOGRAM'
        position: A1 notation position where to place the chart (default: 'A1')
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not chart_title or not data_range:
        return {
            "successful": False,
            "message": "Error: Chart title and data range must be specified",
            "error": "Missing required parameters"
        }
    
    # Validate chart type
    valid_chart_types = ['COLUMN', 'LINE', 'PIE', 'BAR', 'AREA', 'SCATTER', 'HISTOGRAM']
    if chart_type.upper() not in valid_chart_types:
        return {
            "successful": False,
            "message": f"Error: Invalid chart type '{chart_type}'. Valid types: {', '.join(valid_chart_types)}",
            "error": "Invalid chart type",
            "valid_types": valid_chart_types
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == sheet_name:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [sheet['properties']['title'] for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "available_sheets": available_sheets
            }
        
        # Get the sheet ID
        sheet_id = target_sheet['properties']['sheetId']
        
        # Clean the data range of any quotes
        data_range = data_range.strip().strip("'\"")
        
        # Parse the data range to get actual dimensions
        def parse_range(range_str):
            """Parse A1 notation range to get start/end row and column indices"""
            import re
            # Handle ranges like A1:C10, A:A, 1:5, etc.
            if ':' in range_str:
                start, end = range_str.split(':')
                start_col, start_row = re.match(r'([A-Z]*)(\d*)', start.upper()).groups()
                end_col, end_row = re.match(r'([A-Z]*)(\d*)', end.upper()).groups()
                
                # Convert column letters to indices
                def col_to_index(col_str):
                    if not col_str:
                        return 0
                    col = 0
                    for char in col_str:
                        col = col * 26 + (ord(char) - ord('A') + 1)
                    return col - 1
                
                start_col_idx = col_to_index(start_col)
                end_col_idx = col_to_index(end_col) if end_col else 26
                start_row_idx = int(start_row) - 1 if start_row else 0
                end_row_idx = int(end_row) if end_row else 1000
                
                return start_row_idx, end_row_idx, start_col_idx, end_col_idx
            else:
                # Single cell like A1
                col, row = re.match(r'([A-Z]+)(\d+)', range_str.upper()).groups()
                col_idx = 0
                for char in col:
                    col_idx = col_idx * 26 + (ord(char) - ord('A') + 1)
                col_idx -= 1
                row_idx = int(row) - 1
                return row_idx, row_idx + 1, col_idx, col_idx + 1
        
        # Parse the position to get row and column indices
        def a1_to_indices(a1_notation):
            """Convert A1 notation to row and column indices (0-based)"""
            import re
            match = re.match(r'([A-Z]+)(\d+)', a1_notation.upper())
            if not match:
                return 0, 0  # Default to A1 if parsing fails
            
            col_str, row_str = match.groups()
            col = 0
            for char in col_str:
                col = col * 26 + (ord(char) - ord('A') + 1)
            col -= 1  # Convert to 0-based index
            
            row = int(row_str) - 1  # Convert to 0-based index
            return row, col
        
        # Parse the data range
        start_row, end_row, start_col, end_col = parse_range(data_range)
        chart_row, chart_col = a1_to_indices(position)
        
        # Create the chart request with proper source ranges
        # Check if we have multiple columns for series data
        if end_col > start_col + 1:
            # Multiple columns - use first for categories, rest for series
            chart_request = {
                'addChart': {
                    'chart': {
                        'spec': {
                            'title': chart_title,
                            'basicChart': {
                                'chartType': chart_type.upper(),
                                'legendPosition': 'BOTTOM_LEGEND',
                                'axis': [
                                    {
                                        'position': 'BOTTOM_AXIS',
                                        'title': 'Categories'
                                    },
                                    {
                                        'position': 'LEFT_AXIS',
                                        'title': 'Values'
                                    }
                                ],
                                'domains': [
                                    {
                                        'domain': {
                                            'sourceRange': {
                                                'sources': [{
                                                    'sheetId': sheet_id,
                                                    'startRowIndex': start_row,
                                                    'endRowIndex': end_row,
                                                    'startColumnIndex': start_col,
                                                    'endColumnIndex': start_col + 1
                                                }]
                                            }
                                        }
                                    }
                                ],
                                'series': [
                                    {
                                        'series': {
                                            'sourceRange': {
                                                'sources': [{
                                                    'sheetId': sheet_id,
                                                    'startRowIndex': start_row,
                                                    'endRowIndex': end_row,
                                                    'startColumnIndex': start_col + 1,
                                                    'endColumnIndex': end_col
                                                }]
                                            }
                                        },
                                        'targetAxis': 'LEFT_AXIS'
                                    }
                                ]
                            }
                        },
                        'position': {
                            'overlayPosition': {
                                'anchorCell': {
                                    'sheetId': sheet_id,
                                    'rowIndex': chart_row,
                                    'columnIndex': chart_col
                                },
                                'offsetXPixels': 0,
                                'offsetYPixels': 0,
                                'widthPixels': 600,
                                'heightPixels': 400
                            }
                        }
                    }
                }
            }
        else:
            # Single column - create a simple chart with just the data
            chart_request = {
                'addChart': {
                    'chart': {
                        'spec': {
                            'title': chart_title,
                            'basicChart': {
                                'chartType': chart_type.upper(),
                                'legendPosition': 'BOTTOM_LEGEND',
                                'axis': [
                                    {
                                        'position': 'BOTTOM_AXIS',
                                        'title': 'Categories'
                                    },
                                    {
                                        'position': 'LEFT_AXIS',
                                        'title': 'Values'
                                    }
                                ],
                                'domains': [
                                    {
                                        'domain': {
                                            'sourceRange': {
                                                'sources': [{
                                                    'sheetId': sheet_id,
                                                    'startRowIndex': start_row,
                                                    'endRowIndex': end_row,
                                                    'startColumnIndex': start_col,
                                                    'endColumnIndex': end_col
                                                }]
                                            }
                                        }
                                    }
                                ],
                                'series': [
                                    {
                                        'series': {
                                            'sourceRange': {
                                                'sources': [{
                                                    'sheetId': sheet_id,
                                                    'startRowIndex': start_row,
                                                    'endRowIndex': end_row,
                                                    'startColumnIndex': start_col,
                                                    'endColumnIndex': end_col
                                                }]
                                            }
                                        },
                                        'targetAxis': 'LEFT_AXIS'
                                    }
                                ]
                            }
                        },
                        'position': {
                            'overlayPosition': {
                                'anchorCell': {
                                    'sheetId': sheet_id,
                                    'rowIndex': chart_row,
                                    'columnIndex': chart_col
                                },
                                'offsetXPixels': 0,
                                'offsetYPixels': 0,
                                'widthPixels': 600,
                                'heightPixels': 400
                            }
                        }
                    }
                }
            }
        
        # Execute the chart creation request
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [chart_request]
            }
        ).execute()
        
        # Get the created chart ID
        chart_id = response['replies'][0]['addChart']['chart']['chartId']
        
        return {
            "successful": True,
            "message": f"Successfully created {chart_type.lower()} chart '{chart_title}' in sheet '{sheet_name}' at position {position}. Chart ID: {chart_id}",
            "chart_id": chart_id,
            "chart_title": chart_title,
            "sheet_name": sheet_name,
            "chart_type": chart_type,
            "position": position
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error creating chart: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def create_google_sheet(title: str) -> dict:
    """Creates a new Google Spreadsheet in Google Drive using the provided title.
    
    Args:
        title: The title for the new Google Sheet. This will be the name of the file in Google Drive.
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not title:
        return {
            "successful": False,
            "message": "Error: Sheet title must be specified",
            "error": "Missing title parameter"
        }
    
    try:
        # Build the Drive service
        service = build('drive', 'v3', credentials=creds)
        
        # Create the new spreadsheet
        file_metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.spreadsheet'
        }
        
        file = service.files().create(
            body=file_metadata,
            fields='id,name,webViewLink'
        ).execute()
        
        # Get the file details
        file_id = file.get('id')
        file_name = file.get('name')
        web_link = file.get('webViewLink')
        
        return {
            "successful": True,
            "message": f"Successfully created Google Sheet '{file_name}' with ID: {file_id}",
            "spreadsheet_id": file_id,
            "title": file_name,
            "web_view_link": web_link,
            "file_id": file_id
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error creating Google Sheet: {str(e)}",
            "error": str(e),
            "spreadsheet_id": None,
            "title": None,
            "web_view_link": None,
            "file_id": None
        }


@simple_mcp.tool()
async def create_spreadsheet_column(spreadsheet_id: str, sheet_id: int, insert_index: int = None) -> dict:
    """Creates a new column in a Google Spreadsheet, requiring a valid spreadsheet ID and an existing sheet ID. An out-of-bounds insert index may append/prepend the column.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_id: The ID of the specific sheet/tab to modify (integer)
        insert_index: The column index where to insert the new column (0-based). 
                     If None, appends to the end. If out-of-bounds, appends/prepends accordingly.
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "error": "Missing spreadsheet ID"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == sheet_id:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet with ID {sheet_id} not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "sheet_id": sheet_id,
                "available_sheets": available_sheets
            }
        
        # Get current sheet dimensions
        current_col_count = target_sheet['properties']['gridProperties']['columnCount']
        
        # Determine the insert index
        if insert_index is None:
            # Append to the end
            insert_index = current_col_count
        elif insert_index < 0:
            # Prepend to the beginning
            insert_index = 0
        elif insert_index > current_col_count:
            # Append to the end if out of bounds
            insert_index = current_col_count
        
        # Create the insert dimension request
        insert_request = {
            'insertDimension': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'COLUMNS',
                    'startIndex': insert_index,
                    'endIndex': insert_index + 1
                },
                'inheritFromBefore': False
            }
        }
        
        # Execute the insert request
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [insert_request]
            }
        ).execute()
        
        # Get the new column position
        new_col_letter = chr(ord('A') + insert_index)
        
        return {
            "successful": True,
            "message": f"Successfully created new column at position {new_col_letter} (index {insert_index}) in sheet '{target_sheet['properties']['title']}'",
            "column_position": new_col_letter,
            "column_index": insert_index,
            "sheet_name": target_sheet['properties']['title']
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error creating spreadsheet column: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def create_spreadsheet_row(spreadsheet_id: str, sheet_id: int, insert_index: int = None, inherit_formatting: bool = True) -> dict:
    """Inserts a new, empty row into a specified sheet of a Google spreadsheet at a given index, optionally inheriting formatting from the row above.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_id: The ID of the specific sheet/tab to modify (integer)
        insert_index: The row index where to insert the new row (0-based). 
                     If None, appends to the end. If out-of-bounds, appends/prepends accordingly.
        inherit_formatting: Whether to inherit formatting from the row above (default: True)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "error": "Missing spreadsheet ID"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == sheet_id:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet with ID {sheet_id} not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "sheet_id": sheet_id,
                "available_sheets": available_sheets
            }
        
        # Get current sheet dimensions
        current_row_count = target_sheet['properties']['gridProperties']['rowCount']
        
        # Determine the insert index
        if insert_index is None:
            # Append to the end
            insert_index = current_row_count
        elif insert_index < 0:
            # Prepend to the beginning
            insert_index = 0
        elif insert_index > current_row_count:
            # Append to the end if out of bounds
            insert_index = current_row_count
        
        # Determine whether to inherit formatting from above or below
        # If inserting at the beginning, we can't inherit from above, so inherit from below
        if insert_index == 0:
            inherit_from_before = False
        else:
            inherit_from_before = inherit_formatting
        
        # Create the insert dimension request
        insert_request = {
            'insertDimension': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': 'ROWS',
                    'startIndex': insert_index,
                    'endIndex': insert_index + 1
                },
                'inheritFromBefore': inherit_from_before
            }
        }
        
        # Execute the insert request
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [insert_request]
            }
        ).execute()
        
        # Get the new row position (1-based for user display)
        new_row_number = insert_index + 1
        
        # Determine formatting source
        if insert_index == 0:
            formatting_source = "below (first row)"
        elif inherit_formatting:
            formatting_source = "above"
        else:
            formatting_source = "default"
        
        return {
            "successful": True,
            "message": f"Successfully created new row at position {new_row_number} (index {insert_index}) in sheet '{target_sheet['properties']['title']}' with formatting inherited from {formatting_source}",
            "row_number": new_row_number,
            "row_index": insert_index,
            "sheet_name": target_sheet['properties']['title'],
            "formatting_source": formatting_source
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error creating spreadsheet row: {str(e)}",
            "error": str(e)
        }


@simple_mcp.tool()
async def delete_dimension(spreadsheet_id: str, sheet_id: int, dimension: str, start_index: int, end_index: int) -> dict:
    """Tool to delete specified rows or columns from a sheet in a Google spreadsheet. Use when you need to remove a range of rows or columns.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_id: The ID of the specific sheet/tab to modify (integer)
        dimension: The dimension to delete - either 'ROWS' or 'COLUMNS'
        start_index: The starting index of the range to delete (0-based, inclusive)
        end_index: The ending index of the range to delete (0-based, exclusive)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "error": "Missing spreadsheet ID"
        }
    
    # Validate dimension parameter
    if dimension.upper() not in ['ROWS', 'COLUMNS']:
        return {
            "successful": False,
            "message": "Error: dimension must be either 'ROWS' or 'COLUMNS'",
            "error": "Invalid dimension parameter",
            "valid_dimensions": ['ROWS', 'COLUMNS']
        }
    
    # Validate index parameters
    if start_index < 0:
        return {
            "successful": False,
            "message": "Error: start_index must be non-negative",
            "error": "Invalid start_index"
        }
    
    if end_index <= start_index:
        return {
            "successful": False,
            "message": "Error: end_index must be greater than start_index",
            "error": "Invalid end_index"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == sheet_id:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet with ID {sheet_id} not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "sheet_id": sheet_id,
                "available_sheets": available_sheets
            }
        
        # Get current sheet dimensions
        current_row_count = target_sheet['properties']['gridProperties']['rowCount']
        current_col_count = target_sheet['properties']['gridProperties']['columnCount']
        
        # Validate indices against current dimensions
        if dimension.upper() == 'ROWS':
            if end_index > current_row_count:
                return {
                    "successful": False,
                    "message": f"Error: end_index ({end_index}) exceeds current row count ({current_row_count})",
                    "error": "Index out of bounds",
                    "end_index": end_index,
                    "current_row_count": current_row_count,
                    "dimension": dimension
                }
            dimension_name = "rows"
            start_display = start_index + 1  # Convert to 1-based for user display
            end_display = end_index
        else:  # COLUMNS
            if end_index > current_col_count:
                return {
                    "successful": False,
                    "message": f"Error: end_index ({end_index}) exceeds current column count ({current_col_count})",
                    "error": "Index out of bounds",
                    "end_index": end_index,
                    "current_column_count": current_col_count,
                    "dimension": dimension
                }
            dimension_name = "columns"
            start_display = chr(ord('A') + start_index)  # Convert to column letter
            end_display = chr(ord('A') + end_index - 1)
        
        # Create the delete dimension request
        delete_request = {
            'deleteDimension': {
                'range': {
                    'sheetId': sheet_id,
                    'dimension': dimension.upper(),
                    'startIndex': start_index,
                    'endIndex': end_index
                }
            }
        }
        
        # Execute the delete request
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [delete_request]
            }
        ).execute()
        
        # Calculate the number of deleted items
        deleted_count = end_index - start_index
        
        return {
            "successful": True,
            "message": f"Successfully deleted {deleted_count} {dimension_name} from sheet '{target_sheet['properties']['title']}' (range: {start_display} to {end_display})",
            "deleted_count": deleted_count,
            "dimension": dimension_name,
            "sheet_name": target_sheet['properties']['title'],
            "range": f"{start_display} to {end_display}"
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error deleting {dimension.lower()}: {str(e)}",
            "error": str(e),
            "dimension": dimension
        }


@simple_mcp.tool()
async def delete_spreadsheet(spreadsheet_id: str) -> dict:
    """Tool to delete an entire Google Spreadsheet from Google Drive. Use when you need to permanently remove a spreadsheet document.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL) to delete
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "error": "Missing spreadsheet ID"
        }
    
    try:
        # Build the Drive service
        service = build('drive', 'v3', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            file = service.files().get(
                fileId=spreadsheet_id,
                fields='id,name,mimeType,trashed'
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Verify it's actually a Google Spreadsheet
        if file.get('mimeType') != 'application/vnd.google-apps.spreadsheet':
            return {
                "successful": False,
                "message": f"Error: The specified ID does not correspond to a Google Spreadsheet. File type: {file.get('mimeType', 'Unknown')}",
                "error": "Invalid file type",
                "file_type": file.get('mimeType', 'Unknown'),
                "spreadsheet_id": spreadsheet_id
            }
        
        # Check if file is already in trash
        if file.get('trashed', False):
            return {
                "successful": False,
                "message": f"Error: The spreadsheet '{file.get('name')}' is already in the trash",
                "error": "Already deleted",
                "file_name": file.get('name'),
                "spreadsheet_id": spreadsheet_id
            }
        
        # Get file details for confirmation
        file_name = file.get('name')
        
        # Delete the spreadsheet
        service.files().delete(fileId=spreadsheet_id).execute()
        
        return {
            "successful": True,
            "message": f"Successfully deleted Google Spreadsheet '{file_name}' (ID: {spreadsheet_id}) from Google Drive",
            "file_name": file_name,
            "spreadsheet_id": spreadsheet_id
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error deleting spreadsheet: {str(e)}",
            "error": str(e),
            "spreadsheet_id": spreadsheet_id
        }


@simple_mcp.tool()
async def delete_sheet(spreadsheet_id: str, sheet_id: int) -> dict:
    """Tool to delete a sheet (worksheet) from a spreadsheet. Use when you need to remove a specific sheet from a Google sheet document.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        sheet_id: The ID of the specific sheet/tab to delete (integer)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "error": "Missing spreadsheet ID"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Find the specified sheet
        target_sheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == sheet_id:
                target_sheet = sheet
                break
        
        if not target_sheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Sheet with ID {sheet_id} not found. Available sheets: {', '.join(available_sheets)}",
                "error": "Sheet not found",
                "sheet_id": sheet_id,
                "available_sheets": available_sheets
            }
        
        # Prevent deletion of the last sheet (Google Sheets requires at least one sheet)
        if len(spreadsheet['sheets']) == 1:
            return {
                "successful": False,
                "message": "Error: Cannot delete the last remaining sheet. Use delete_spreadsheet instead.",
                "error": "Cannot delete last sheet",
                "sheet_id": sheet_id,
                "spreadsheet_id": spreadsheet_id
            }
        
        # Get sheet details for confirmation
        sheet_title = target_sheet['properties']['title']
        sheet_index = target_sheet['properties']['index']
        
        # Create the delete sheet request
        delete_request = {
            'deleteSheet': {
                'sheetId': sheet_id
            }
        }
        
        # Execute the delete request
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [delete_request]
            }
        ).execute()
        
        return {
            "successful": True,
            "message": f"Successfully deleted sheet '{sheet_title}' (ID: {sheet_id}, was at position {sheet_index + 1}) from the spreadsheet",
            "sheet_title": sheet_title,
            "sheet_id": sheet_id,
            "position": sheet_index + 1,
            "spreadsheet_id": spreadsheet_id
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error deleting sheet: {str(e)}",
            "error": str(e),
            "sheet_id": sheet_id,
            "spreadsheet_id": spreadsheet_id
        }


@simple_mcp.tool()
async def find_worksheet_by_title(spreadsheet_id: str, title: str) -> dict:
    """Finds a worksheet by its exact, case-sensitive title within a Google spreadsheet. Returns a boolean indicating if found and the complete metadata of the entire spreadsheet, regardless of whether the target worksheet is found.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        title: The exact, case-sensitive title of the worksheet (tab name) to find
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "found": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "worksheet_info": None,
            "spreadsheet_metadata": None,
            "all_worksheets": []
        }
    
    if not spreadsheet_id:
        return {
            "found": False,
            "message": "Error: Spreadsheet ID must be specified",
            "worksheet_info": None,
            "spreadsheet_metadata": None,
            "all_worksheets": []
        }
    
    if not title:
        return {
            "found": False,
            "message": "Error: Worksheet title must be specified",
            "worksheet_info": None,
            "spreadsheet_metadata": None,
            "all_worksheets": []
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get the complete spreadsheet metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "found": False,
                "message": f"Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "worksheet_info": None,
                "spreadsheet_metadata": None,
                "all_worksheets": []
            }
        
        # Check if spreadsheet has any worksheets
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return {
                "found": False,
                "message": "Spreadsheet has no worksheets",
                "worksheet_info": None,
                "spreadsheet_metadata": {
                    "spreadsheet_id": spreadsheet_id,
                    "properties": spreadsheet.get('properties', {}),
                    "sheets": []
                },
                "all_worksheets": []
            }
        
        # Extract spreadsheet metadata
        spreadsheet_metadata = {
            "spreadsheet_id": spreadsheet_id,
            "properties": spreadsheet.get('properties', {}),
            "sheets": []
        }
        
        # Get all worksheet information
        all_worksheets = []
        target_worksheet = None
        
        for sheet in spreadsheet['sheets']:
            sheet_props = sheet.get('properties', {})
            sheet_title = sheet_props.get('title', '')
            sheet_id = sheet_props.get('sheetId', 0)
            sheet_index = sheet_props.get('index', 0)
            
            worksheet_info = {
                "title": sheet_title,
                "sheet_id": sheet_id,
                "index": sheet_index,
                "hidden": sheet_props.get('hidden', False),
                "grid_properties": sheet_props.get('gridProperties', {})
            }
            
            all_worksheets.append(worksheet_info)
            spreadsheet_metadata["sheets"].append(worksheet_info)
            
            # Check for exact match (case-sensitive)
            if sheet_title == title:
                target_worksheet = worksheet_info
        
        # Prepare response
        if target_worksheet:
            return {
                "found": True,
                "message": f"Worksheet '{title}' found successfully",
                "worksheet_info": target_worksheet,
                "spreadsheet_metadata": spreadsheet_metadata,
                "all_worksheets": all_worksheets
            }
        else:
            # Worksheet not found, but return complete metadata
            available_titles = [ws["title"] for ws in all_worksheets]
            return {
                "found": False,
                "message": f"Worksheet '{title}' not found. Available worksheets: {', '.join(available_titles)}",
                "worksheet_info": None,
                "spreadsheet_metadata": spreadsheet_metadata,
                "all_worksheets": all_worksheets
            }
        
    except Exception as e:
        return {
            "found": False,
            "message": f"Error finding worksheet: {str(e)}",
            "worksheet_info": None,
            "spreadsheet_metadata": None,
            "all_worksheets": []
        }


@simple_mcp.tool()
async def format_cell(spreadsheet_id: str, worksheet_id: int, start_row_index: int, end_row_index: int, start_column_index: int, end_column_index: int, bold: bool = False, italic: bool = False, underline: bool = False, strikethrough: bool = False, fontSize: int = 10, red: float = 0.9, green: float = 0.9, blue: float = 0.9) -> dict:
    """Applies text and background cell formatting to a specified range in a Google Sheets worksheet.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        worksheet_id: The ID of the specific worksheet/tab to format (integer)
        start_row_index: The starting row index of the range to format (0-based, inclusive)
        end_row_index: The ending row index of the range to format (0-based, exclusive)
        start_column_index: The starting column index of the range to format (0-based, inclusive)
        end_column_index: The ending column index of the range to format (0-based, exclusive)
        bold: Whether to make text bold (default: False)
        italic: Whether to make text italic (default: False)
        underline: Whether to underline text (default: False)
        strikethrough: Whether to strikethrough text (default: False)
        fontSize: Font size in points (default: 10)
        red: Red component of background color (0.0-1.0, default: 0.9)
        green: Green component of background color (0.0-1.0, default: 0.9)
        blue: Blue component of background color (0.0-1.0, default: 0.9)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"]
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "error": "Missing spreadsheet ID"
        }
    
    # Validate index parameters
    if start_row_index < 0 or end_row_index < 0:
        return {
            "successful": False,
            "message": "Error: Row indices must be non-negative",
            "error": "Invalid row indices"
        }
    
    if start_column_index < 0 or end_column_index < 0:
        return {
            "successful": False,
            "message": "Error: Column indices must be non-negative",
            "error": "Invalid column indices"
        }
    
    if end_row_index <= start_row_index:
        return {
            "successful": False,
            "message": "Error: end_row_index must be greater than start_row_index",
            "error": "Invalid row range"
        }
    
    if end_column_index <= start_column_index:
        return {
            "successful": False,
            "message": "Error: end_column_index must be greater than start_column_index",
            "error": "Invalid column range"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, verify the spreadsheet exists and get its metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error: Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "error": str(e)
            }
        
        # Check if spreadsheet has at least one worksheet
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return "Error: Spreadsheet has no worksheets"
        
        # Find the specified worksheet
        target_worksheet = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == worksheet_id:
                target_worksheet = sheet
                break
        
        if not target_worksheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" for sheet in spreadsheet['sheets']]
            return {
                "successful": False,
                "message": f"Error: Worksheet with ID {worksheet_id} not found. Available worksheets: {', '.join(available_sheets)}",
                "error": "Worksheet not found",
                "worksheet_id": worksheet_id,
                "available_worksheets": available_sheets
            }
        
        # Get worksheet details for confirmation
        worksheet_title = target_worksheet['properties']['title']
        
        # Create the format request
        format_request = {
            'repeatCell': {
                'range': {
                    'sheetId': worksheet_id,
                    'startRowIndex': start_row_index,
                    'endRowIndex': end_row_index,
                    'startColumnIndex': start_column_index,
                    'endColumnIndex': end_column_index
                },
                'cell': {
                    'userEnteredFormat': {
                        'backgroundColor': {
                            'red': red,
                            'green': green,
                            'blue': blue
                        },
                        'textFormat': {
                            'bold': bold,
                            'italic': italic,
                            'underline': underline,
                            'strikethrough': strikethrough,
                            'fontSize': fontSize
                        }
                    }
                },
                'fields': 'userEnteredFormat.backgroundColor,userEnteredFormat.textFormat'
            }
        }
        
        # Execute the format request
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                'requests': [format_request]
            }
        ).execute()
        
        # Calculate the formatted range
        start_row_display = start_row_index + 1  # Convert to 1-based for user display
        end_row_display = end_row_index
        start_col_display = chr(ord('A') + start_column_index)
        end_col_display = chr(ord('A') + end_column_index - 1)
        
        return {
            "successful": True,
            "message": f"Successfully formatted range {start_col_display}{start_row_display}:{end_col_display}{end_row_display} in worksheet '{worksheet_title}'",
            "range": f"{start_col_display}{start_row_display}:{end_col_display}{end_row_display}",
            "worksheet_title": worksheet_title,
            "worksheet_id": worksheet_id
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error formatting cells: {str(e)}",
            "error": str(e),
            "worksheet_id": worksheet_id
        }


@simple_mcp.tool()
async def get_sheet_names(spreadsheet_id: str) -> dict:
    """Lists all worksheet names from a specified Google spreadsheet (which must exist), useful for discovering sheets before further operations.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "sheet_names": [],
            "sheet_count": 0,
            "spreadsheet_info": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "sheet_names": [],
            "sheet_count": 0,
            "spreadsheet_info": None
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get the spreadsheet metadata
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "sheet_names": [],
                "sheet_count": 0,
                "spreadsheet_info": None
            }
        
        # Extract spreadsheet information
        spreadsheet_props = spreadsheet.get('properties', {})
        spreadsheet_info = {
            "spreadsheet_id": spreadsheet_id,
            "title": spreadsheet_props.get('title', 'Unknown'),
            "locale": spreadsheet_props.get('locale', 'Unknown'),
            "time_zone": spreadsheet_props.get('timeZone', 'Unknown'),
            "auto_recalc": spreadsheet_props.get('autoRecalc', 'Unknown')
        }
        
        # Check if spreadsheet has any worksheets
        if 'sheets' not in spreadsheet or len(spreadsheet['sheets']) == 0:
            return {
                "successful": True,
                "message": "Spreadsheet has no worksheets",
                "sheet_names": [],
                "sheet_count": 0,
                "spreadsheet_info": spreadsheet_info
            }
        
        # Extract all sheet names and details
        sheet_names = []
        sheet_details = []
        
        for sheet in spreadsheet['sheets']:
            sheet_props = sheet.get('properties', {})
            sheet_title = sheet_props.get('title', '')
            sheet_id = sheet_props.get('sheetId', 0)
            sheet_index = sheet_props.get('index', 0)
            
            sheet_names.append(sheet_title)
            sheet_details.append({
                "title": sheet_title,
                "sheet_id": sheet_id,
                "index": sheet_index,
                "hidden": sheet_props.get('hidden', False)
            })
        
        # Sort by index to maintain order
        sheet_details.sort(key=lambda x: x['index'])
        sheet_names = [sheet['title'] for sheet in sheet_details]
        
        return {
            "successful": True,
            "message": f"Found {len(sheet_names)} worksheet(s) in spreadsheet '{spreadsheet_info['title']}'",
            "sheet_names": sheet_names,
            "sheet_count": len(sheet_names),
            "sheet_details": sheet_details,
            "spreadsheet_info": spreadsheet_info
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error getting sheet names: {str(e)}",
            "sheet_names": [],
            "sheet_count": 0,
            "spreadsheet_info": None
        }


@simple_mcp.tool()
async def get_spreadsheet_by_data_filter(spreadsheet_id: str, data_filters: list, include_grid_data: bool = False, exclude_tables_in_banded_ranges: bool = False) -> dict:
    """Returns the spreadsheet at the given id, filtered by the specified data filters. Use this tool when you need to retrieve specific subsets of data from a Google sheet based on criteria like A1 notation, developer metadata, or grid ranges.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        data_filters: List of data filters to apply. Each filter can be:
                     - A1 notation range string (e.g., "Sheet1!A1:C10", "To do!A9:C11")
                     - DataFilter object with a1Range, gridRange, or developerMetadataLookup
        include_grid_data: Whether to include the actual data in the response (default: False)
        exclude_tables_in_banded_ranges: Whether to exclude tables in banded ranges (default: False)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheet_data": None,
            "filtered_ranges": []
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheet_data": None,
            "filtered_ranges": [],
            "error": "Missing spreadsheet ID"
        }
    
    if not data_filters or len(data_filters) == 0:
        return {
            "successful": False,
            "message": "Error: At least one data filter must be specified",
            "spreadsheet_data": None,
            "filtered_ranges": [],
            "error": "No data filters provided"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Convert A1 notation filters to proper DataFilter objects
        processed_filters = []
        for filter_item in data_filters:
            if isinstance(filter_item, str):
                # Convert A1 notation to DataFilter object
                if '!' in filter_item:
                    sheet_name, range_part = filter_item.split('!', 1)
                    processed_filters.append({
                        'a1Range': filter_item
                    })
                else:
                    # No sheet name specified, use first sheet
                    processed_filters.append({
                        'a1Range': f"Sheet1!{filter_item}"
                    })
            elif isinstance(filter_item, dict):
                # Already a proper DataFilter object
                processed_filters.append(filter_item)
            else:
                return {
                    "successful": False,
                    "message": f"Invalid data filter format: {filter_item}. Must be string (A1 notation) or dict (DataFilter object)",
                    "spreadsheet_data": None,
                    "filtered_ranges": [],
                    "error": "Invalid filter format"
                }
        
        # Prepare the request body
        request_body = {
            'dataFilters': processed_filters,
            'includeGridData': include_grid_data,
            'excludeTablesInBandedRanges': exclude_tables_in_banded_ranges
        }
        
        # Get the filtered spreadsheet data
        try:
            response = service.spreadsheets().getByDataFilter(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not retrieve filtered spreadsheet data. Make sure the ID is correct and you have access. Details: {str(e)}",
                "spreadsheet_data": None,
                "filtered_ranges": [],
                "error": str(e)
            }
        
        # Extract spreadsheet information
        spreadsheet_props = response.get('properties', {})
        spreadsheet_info = {
            "spreadsheet_id": spreadsheet_id,
            "title": spreadsheet_props.get('title', 'Unknown'),
            "locale": spreadsheet_props.get('locale', 'Unknown'),
            "time_zone": spreadsheet_props.get('timeZone', 'Unknown'),
            "auto_recalc": spreadsheet_props.get('autoRecalc', 'Unknown')
        }
        
        # Extract filtered sheets data
        filtered_sheets = response.get('sheets', [])
        filtered_ranges = []
        
        for sheet in filtered_sheets:
            sheet_props = sheet.get('properties', {})
            sheet_title = sheet_props.get('title', 'Unknown')
            sheet_id = sheet_props.get('sheetId', 0)
            
            # Get filtered ranges for this sheet
            sheet_data = sheet.get('data', [])
            for data_range in sheet_data:
                range_info = {
                    "sheet_title": sheet_title,
                    "sheet_id": sheet_id,
                    "start_row_index": data_range.get('startRowIndex', 0),
                    "end_row_index": data_range.get('endRowIndex', 0),
                    "start_column_index": data_range.get('startColumnIndex', 0),
                    "end_column_index": data_range.get('endColumnIndex', 0),
                    "row_data": []
                }
                
                # Extract row data if include_grid_data is True
                if include_grid_data:
                    row_data = data_range.get('rowData', [])
                    for row in row_data:
                        row_values = []
                        values = row.get('values', [])
                        for value in values:
                            # Extract the actual value from the cell
                            cell_value = value.get('formattedValue', '')
                            if not cell_value:
                                # Try other value formats
                                user_entered_value = value.get('userEnteredValue', {})
                                if 'stringValue' in user_entered_value:
                                    cell_value = user_entered_value['stringValue']
                                elif 'numberValue' in user_entered_value:
                                    cell_value = str(user_entered_value['numberValue'])
                                elif 'boolValue' in user_entered_value:
                                    cell_value = str(user_entered_value['boolValue'])
                                else:
                                    cell_value = ''
                            row_values.append(cell_value)
                        range_info["row_data"].append(row_values)
                
                filtered_ranges.append(range_info)
        
        # Format the response
        result = {
            "successful": True,
            "message": f"Successfully retrieved filtered data from spreadsheet '{spreadsheet_info['title']}'",
            "spreadsheet_info": spreadsheet_info,
            "filtered_ranges": filtered_ranges,
            "data_filters_applied": data_filters,
            "include_grid_data": include_grid_data,
            "exclude_tables_in_banded_ranges": exclude_tables_in_banded_ranges,
            "total_ranges_found": len(filtered_ranges)
        }
        
        return result
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error retrieving filtered spreadsheet data: {str(e)}",
            "spreadsheet_data": None,
            "filtered_ranges": [],
            "error": str(e)
        }


@simple_mcp.tool()
async def get_spreadsheet_info(spreadsheet_id: str) -> dict:
    """Retrieves comprehensive metadata for a Google spreadsheet using its ID, excluding cell data.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheet_info": None,
            "sheets_info": []
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheet_info": None,
            "sheets_info": [],
            "error": "Missing spreadsheet ID"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get the spreadsheet metadata (excluding cell data)
        try:
            response = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],  # Empty ranges to exclude cell data
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "spreadsheet_info": None,
                "sheets_info": [],
                "error": str(e)
            }
        
        # Extract comprehensive spreadsheet information
        spreadsheet_props = response.get('properties', {})
        spreadsheet_info = {
            "spreadsheet_id": spreadsheet_id,
            "title": spreadsheet_props.get('title', 'Unknown'),
            "locale": spreadsheet_props.get('locale', 'Unknown'),
            "time_zone": spreadsheet_props.get('timeZone', 'Unknown'),
            "auto_recalc": spreadsheet_props.get('autoRecalc', 'Unknown'),
            "default_format": spreadsheet_props.get('defaultFormat', {}),
            "iterative_calculation_settings": spreadsheet_props.get('iterativeCalculationSettings', {}),
            "named_ranges": response.get('namedRanges', []),
            "developer_metadata": response.get('developerMetadata', []),
            "data_source_specs": response.get('dataSourceSpecs', []),
            "data_execution_status": response.get('dataExecutionStatus', {}),
            "theme": response.get('theme', {}),
            "spreadsheet_theme": response.get('spreadsheetTheme', {}),
            "sheets_count": len(response.get('sheets', [])),
            "last_modified": spreadsheet_props.get('updated', 'Unknown'),
            "created": spreadsheet_props.get('created', 'Unknown'),
            "owner": spreadsheet_props.get('owner', 'Unknown'),
            "editors": spreadsheet_props.get('editors', []),
            "viewers": spreadsheet_props.get('viewers', []),
            "permissions": response.get('permissions', [])
        }
        
        # Extract detailed information about each sheet
        sheets_info = []
        for sheet in response.get('sheets', []):
            sheet_props = sheet.get('properties', {})
            sheet_info = {
                "title": sheet_props.get('title', 'Unknown'),
                "sheet_id": sheet_props.get('sheetId', 0),
                "index": sheet_props.get('index', 0),
                "sheet_type": sheet_props.get('sheetType', 'Unknown'),
                "hidden": sheet_props.get('hidden', False),
                "right_to_left": sheet_props.get('rightToLeft', False),
                "tab_color": sheet_props.get('tabColor', {}),
                "grid_properties": {
                    "row_count": sheet_props.get('gridProperties', {}).get('rowCount', 0),
                    "column_count": sheet_props.get('gridProperties', {}).get('columnCount', 0),
                    "frozen_row_count": sheet_props.get('gridProperties', {}).get('frozenRowCount', 0),
                    "frozen_column_count": sheet_props.get('gridProperties', {}).get('frozenColumnCount', 0),
                    "hide_gridlines": sheet_props.get('gridProperties', {}).get('hideGridlines', False)
                },
                "basic_filter": sheet.get('basicFilter', {}),
                "charts": sheet.get('charts', []),
                "banded_ranges": sheet.get('bandedRanges', []),
                "conditional_formats": sheet.get('conditionalFormats', []),
                "filter_views": sheet.get('filterViews', []),
                "protected_ranges": sheet.get('protectedRanges', []),
                "merges": sheet.get('merges', []),
                "row_groups": sheet.get('rowGroups', []),
                "column_groups": sheet.get('columnGroups', []),
                "slicers": sheet.get('slicers', []),
                "developer_metadata": sheet.get('developerMetadata', [])
            }
            sheets_info.append(sheet_info)
        
        # Sort sheets by index to maintain order
        sheets_info.sort(key=lambda x: x['index'])
        
        return {
            "successful": True,
            "message": f"Successfully retrieved metadata for spreadsheet '{spreadsheet_info['title']}'",
            "spreadsheet_info": spreadsheet_info,
            "sheets_info": sheets_info,
            "total_sheets": len(sheets_info),
            "has_named_ranges": len(spreadsheet_info['named_ranges']) > 0,
            "has_developer_metadata": len(spreadsheet_info['developer_metadata']) > 0,
            "has_data_sources": len(spreadsheet_info['data_source_specs']) > 0
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error retrieving spreadsheet info: {str(e)}",
            "spreadsheet_info": None,
            "sheets_info": [],
            "error": str(e)
        }


@simple_mcp.tool()
async def get_table_schema(spreadsheet_id: str, table_name: str, sheet_name: str | None = None, sample_size: int = 50) -> dict:
    """Get the schema of a table in a Google spreadsheet by analyzing table structure and inferring column names, types, and constraints.
    
    Uses statistical analysis of sample data to determine the most likely data type for each column.
    Call this action after calling the list tables action to get the schema of a table in a spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the Google Sheet (found in the URL)
        table_name: Specific table name from LIST_TABLES response (e.g., 'Sales_Data', 'Employee_List'). Use 'auto' to analyze the largest/most prominent table.
        sheet_name: Sheet/tab name if table_name is ambiguous across multiple sheets
        sample_size: Number of rows to sample for type inference (default: 50, max: 1000)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "schema": None,
            "table_info": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "schema": None,
            "table_info": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not table_name:
        return {
            "successful": False,
            "message": "Error: Table name must be specified",
            "schema": None,
            "table_info": None,
            "error": "Missing table name"
        }
    
    # Validate sample size
    if sample_size < 1 or sample_size > 1000:
        return {
            "successful": False,
            "message": "Error: Sample size must be between 1 and 1000",
            "schema": None,
            "table_info": None,
            "error": "Invalid sample size"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, get spreadsheet metadata to find sheets
        try:
            metadata_response = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "schema": None,
                "table_info": None,
                "error": str(e)
            }
        
        sheets = metadata_response.get('sheets', [])
        if not sheets:
            return {
                "successful": False,
                "message": "No sheets found in the spreadsheet",
                "schema": None,
                "table_info": None,
                "error": "No sheets available"
            }
        
        # Find the target sheet
        target_sheet = None
        if sheet_name:
            # Look for specific sheet name
            for sheet in sheets:
                if sheet.get('properties', {}).get('title', '').strip() == sheet_name.strip():
                    target_sheet = sheet
                    break
            if not target_sheet:
                available_sheets = [s.get('properties', {}).get('title', 'Unknown') for s in sheets]
                return {
                    "successful": False,
                    "message": f"Sheet '{sheet_name}' not found. Available sheets: {', '.join(available_sheets)}",
                    "schema": None,
                    "table_info": None,
                    "error": "Sheet not found"
                }
        else:
            # Use first sheet if no specific sheet name provided
            target_sheet = sheets[0]
        
        sheet_title = target_sheet.get('properties', {}).get('title', 'Unknown')
        sheet_id = target_sheet.get('properties', {}).get('sheetId', 0)
        
        # Get the data from the sheet
        try:
            # Get all data from the sheet
            data_response = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_title}'!A:ZZ"  # Get all columns
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not retrieve data from sheet '{sheet_title}'. Details: {str(e)}",
                "schema": None,
                "table_info": None,
                "error": str(e)
            }
        
        values = data_response.get('values', [])
        if not values:
            return {
                "successful": False,
                "message": f"No data found in sheet '{sheet_title}'",
                "schema": None,
                "table_info": None,
                "error": "No data available"
            }
        
        # Find the table within the sheet
        table_data = None
        table_start_row = 0
        
        if table_name.lower() == 'auto':
            # Analyze the largest table in the sheet
            table_data = values
            table_start_row = 0
        else:
            # Look for the specific table by name
            # This is a simplified approach - in a real implementation, you might want to look for named ranges
            # or use more sophisticated table detection
            table_data = values
            table_start_row = 0
        
        if not table_data or len(table_data) < 2:
            return {
                "successful": False,
                "message": f"Table '{table_name}' not found or insufficient data in sheet '{sheet_title}'",
                "schema": None,
                "table_info": None,
                "error": "Table not found"
            }
        
        # Extract headers (first row)
        headers = table_data[0] if table_data else []
        if not headers:
            return {
                "successful": False,
                "message": "No headers found in the table",
                "schema": None,
                "table_info": None,
                "error": "No headers available"
            }
        
        # Clean headers
        cleaned_headers = [_clean_column_name(header) for header in headers]
        
        # Get sample data for type inference
        sample_rows = table_data[1:min(sample_size + 1, len(table_data))]
        
        # Analyze each column
        schema = []
        for i, (original_header, cleaned_header) in enumerate(zip(headers, cleaned_headers)):
            column_data = []
            for row in sample_rows:
                if i < len(row):
                    value = row[i]
                    column_data.append(value)
            
            # Infer data type
            data_type, constraints = _infer_column_type(column_data, cleaned_header)
            
            schema.append({
                "column_index": i,
                "column_letter": chr(65 + i),  # A, B, C, etc.
                "original_name": original_header,
                "name": cleaned_header,
                "data_type": data_type,
                "constraints": constraints,
                "sample_values": column_data[:10],  # First 10 values as examples
                "null_count": column_data.count(''),
                "unique_count": len(set(filter(None, column_data))),
                "max_length": max(len(str(v)) for v in column_data if v) if column_data else 0
            })
        
        # Calculate table statistics
        total_rows = len(table_data) - 1  # Exclude header
        total_columns = len(schema)
        
        table_info = {
            "table_name": table_name,
            "sheet_name": sheet_title,
            "sheet_id": sheet_id,
            "spreadsheet_id": spreadsheet_id,
            "total_rows": total_rows,
            "total_columns": total_columns,
            "sample_size_used": min(sample_size, total_rows),
            "start_row": table_start_row + 1,  # 1-based for user display
            "end_row": table_start_row + total_rows,
            "start_column": "A",
            "end_column": chr(64 + total_columns),  # A, B, C, etc.
            "range": f"'{sheet_title}'!A{table_start_row + 1}:{chr(64 + total_columns)}{table_start_row + total_rows}"
        }
        
        return {
            "successful": True,
            "message": f"Successfully analyzed schema for table '{table_name}' in sheet '{sheet_title}'",
            "schema": schema,
            "table_info": table_info,
            "analysis_summary": {
                "string_columns": len([col for col in schema if col['data_type'] == 'string']),
                "numeric_columns": len([col for col in schema if col['data_type'] in ['integer', 'float']]),
                "date_columns": len([col for col in schema if col['data_type'] == 'date']),
                "boolean_columns": len([col for col in schema if col['data_type'] == 'boolean']),
                "empty_columns": len([col for col in schema if col['null_count'] == total_rows])
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error analyzing table schema: {str(e)}",
            "schema": None,
            "table_info": None,
            "error": str(e)
        }


def _clean_column_name(column_name: str) -> str:
    """Clean column names by removing leading/trailing whitespace and newlines.
    
    Args:
        column_name: The original column name
        
    Returns:
        Cleaned column name
    """
    if not column_name:
        return ""
    return column_name.strip().strip('\n\r\t')


def _infer_column_type(values: list, column_name: str) -> tuple[str, dict]:
    """Infer the data type of a column based on its values.
    
    Args:
        values: List of values in the column
        column_name: Name of the column for context
    
    Returns:
        Tuple of (data_type, constraints)
    """
    if not values:
        return "string", {}
    
    # Remove empty values for analysis
    non_empty_values = [v for v in values if v != '']
    if not non_empty_values:
        return "string", {}
    
    # Check for boolean values
    boolean_values = ['true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n']
    if all(str(v).lower() in boolean_values for v in non_empty_values):
        return "boolean", {"possible_values": list(set(str(v).lower() for v in non_empty_values))}
    
    # Check for dates
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
        r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
        r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
        r'\d{1,2}/\d{1,2}/\d{2,4}',  # M/D/YY or M/D/YYYY
    ]
    
    import re
    date_count = 0
    for value in non_empty_values:
        value_str = str(value)
        for pattern in date_patterns:
            if re.match(pattern, value_str):
                date_count += 1
                break
    
    if date_count >= len(non_empty_values) * 0.8:  # 80% match rate
        return "date", {"format": "various"}
    
    # Check for numbers
    numeric_count = 0
    float_count = 0
    for value in non_empty_values:
        value_str = str(value).replace(',', '').replace('$', '').replace('%', '')
        try:
            float_val = float(value_str)
            numeric_count += 1
            if float_val != int(float_val):
                float_count += 1
        except ValueError:
            pass
    
    if numeric_count >= len(non_empty_values) * 0.9:  # 90% numeric
        if float_count > 0:
            return "float", {"precision": "decimal"}
        else:
            return "integer", {"precision": "whole_number"}
    
    # Check for email addresses
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    email_count = sum(1 for v in non_empty_values if re.match(email_pattern, str(v)))
    if email_count >= len(non_empty_values) * 0.8:
        return "email", {"format": "standard"}
    
    # Check for URLs
    url_pattern = r'^https?://'
    url_count = sum(1 for v in non_empty_values if re.match(url_pattern, str(v)))
    if url_count >= len(non_empty_values) * 0.8:
        return "url", {"protocol": "http/https"}
    
    # Check for phone numbers
    phone_pattern = r'^[\+]?[1-9][\d]{0,15}$'
    phone_count = sum(1 for v in non_empty_values if re.match(phone_pattern, str(v).replace(' ', '').replace('-', '').replace('(', '').replace(')', '')))
    if phone_count >= len(non_empty_values) * 0.8:
        return "phone", {"format": "various"}
    
    # Default to string
    max_length = max(len(str(v)) for v in non_empty_values)
    constraints = {"max_length": max_length}
    
    # Check for categorical data (limited unique values)
    unique_values = set(str(v) for v in non_empty_values)
    if len(unique_values) <= min(10, len(non_empty_values) * 0.3):  # Less than 10 unique values or 30% of data
        constraints["categorical"] = True
        constraints["categories"] = list(unique_values)
    
    return "string", constraints


@simple_mcp.tool()
async def insert_dimension(spreadsheet_id: str, insert_dimension: dict, include_spreadsheet_in_response: bool = False, response_include_grid_data: bool = False, response_ranges: list[str] | None = None) -> dict:
    """Insert new rows or columns into a sheet at a specified location.
    
    Tool to insert new rows or columns into a sheet at a specified location. 
    Use when you need to add empty rows or columns within an existing google sheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet to update
        insert_dimension: The details for the insert dimension request object
            - range: Object containing:
                - sheet_id: The ID of the sheet where the dimensions will be inserted
                - dimension: "ROWS" or "COLUMNS"
                - start_index: The start index (0-based) of the dimension range to insert
                - end_index: The end index (0-based, exclusive) of the dimension range to insert
            Example: {"range": {"sheet_id": 53725981,"dimension": "COLUMNS","start_index": 0,"end_index": 1}}
            - inherit_from_before: If true, new dimensions inherit properties from before startIndex
        include_spreadsheet_in_response: True if the updated spreadsheet should be included in the response
        response_include_grid_data: True if grid data should be included in the response
        response_ranges: Limits the ranges of the spreadsheet to include in the response
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheet": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheet": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not insert_dimension:
        return {
            "successful": False,
            "message": "Error: Insert dimension object must be specified",
            "spreadsheet": None,
            "error": "Missing insert dimension object"
        }
    
    # Validate insert_dimension structure
    if 'range' not in insert_dimension:
        return {
            "successful": False,
            "message": "Error: Insert dimension must contain 'range' object",
            "spreadsheet": None,
            "error": "Missing range in insert dimension"
        }
    
    range_obj = insert_dimension['range']
    required_fields = ['sheet_id', 'dimension', 'start_index', 'end_index']
    for field in required_fields:
        if field not in range_obj:
            return {
                "successful": False,
                "message": f"Error: Range object must contain '{field}' field",
                "spreadsheet": None,
                "error": f"Missing {field} in range object"
            }
    
    # Validate dimension type
    if range_obj['dimension'] not in ['ROWS', 'COLUMNS']:
        return {
            "successful": False,
            "message": "Error: Dimension must be 'ROWS' or 'COLUMNS'",
            "spreadsheet": None,
            "error": "Invalid dimension type"
        }
    
    # Validate indices
    if range_obj['start_index'] < 0 or range_obj['end_index'] <= range_obj['start_index']:
        return {
            "successful": False,
            "message": "Error: start_index must be >= 0 and end_index must be > start_index",
            "spreadsheet": None,
            "error": "Invalid index values"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Prepare the request body
        request_body = {
            'requests': [{
                'insertDimension': insert_dimension
            }]
        }
        
        # Add optional parameters
        if include_spreadsheet_in_response:
            request_body['includeSpreadsheetInResponse'] = include_spreadsheet_in_response
        
        if response_include_grid_data:
            request_body['responseIncludeGridData'] = response_include_grid_data
        
        if response_ranges:
            request_body['responseRanges'] = response_ranges
        
        # Execute the batch update
        try:
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error inserting dimension: {str(e)}",
                "spreadsheet": None,
                "error": str(e)
            }
        
        # Extract response information
        result = {
            "successful": True,
            "message": f"Successfully inserted {range_obj['dimension'].lower()} at position {range_obj['start_index']}",
            "spreadsheet": None,
            "replies": response.get('replies', []),
            "spreadsheet_id": spreadsheet_id,
            "updated_range": response.get('updatedRange'),
            "updated_rows": response.get('updatedRows'),
            "updated_columns": response.get('updatedColumns'),
            "updated_cells": response.get('updatedCells')
        }
        
        # Include spreadsheet data if requested
        if include_spreadsheet_in_response and 'updatedSpreadsheet' in response:
            result['spreadsheet'] = response['updatedSpreadsheet']
        
        # Add dimension details
        dimension_type = range_obj['dimension']
        start_idx = range_obj['start_index']
        end_idx = range_obj['end_index']
        count = end_idx - start_idx
        
        result['inserted_dimension'] = {
            "type": dimension_type,
            "start_index": start_idx,
            "end_index": end_idx,
            "count": count,
            "sheet_id": range_obj['sheet_id'],
            "inherit_from_before": insert_dimension.get('inheritFromBefore', False)
        }
        
        # Add user-friendly description
        if dimension_type == 'ROWS':
            result['description'] = f"Inserted {count} row(s) starting at row {start_idx + 1}"
        else:
            result['description'] = f"Inserted {count} column(s) starting at column {chr(65 + start_idx)}"
        
        return result
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error inserting dimension: {str(e)}",
            "spreadsheet": None,
            "error": str(e)
        }


@simple_mcp.tool()
async def list_tables(spreadsheet_id: str, min_rows: int = 2, min_columns: int = 1, min_confidence: float = 0.5) -> dict:
    """List all tables in a Google spreadsheet by analyzing sheet structure and detecting data patterns.
    
    This action is used to list all tables in a google spreadsheet, call this action to get the list of tables in a spreadsheet. 
    Discover all tables in a google spreadsheet by analyzing sheet structure and detecting data patterns. 
    Uses heuristic analysis to find header rows, data boundaries, and table structures.
    
    Args:
        spreadsheet_id: Google Sheets ID from the URL (e.g., '1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms')
        min_rows: Minimum number of data rows to consider a valid table (default: 2)
        min_columns: Minimum number of columns to consider a valid table (default: 1)
        min_confidence: Minimum confidence score (0.0-1.0) to consider a valid table (default: 0.5)
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "tables": []
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "tables": [],
            "error": "Missing spreadsheet ID"
        }
    
    # Validate parameters
    if min_rows < 1:
        return {
            "successful": False,
            "message": "Error: min_rows must be at least 1",
            "tables": [],
            "error": "Invalid min_rows value"
        }
    
    if min_columns < 1:
        return {
            "successful": False,
            "message": "Error: min_columns must be at least 1",
            "tables": [],
            "error": "Invalid min_columns value"
        }
    
    if min_confidence < 0.0 or min_confidence > 1.0:
        return {
            "successful": False,
            "message": "Error: min_confidence must be between 0.0 and 1.0",
            "tables": [],
            "error": "Invalid min_confidence value"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, get spreadsheet metadata to find sheets
        try:
            metadata_response = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not access spreadsheet. Make sure the ID is correct and you have access. Details: {str(e)}",
                "tables": [],
                "error": str(e)
            }
        
        sheets = metadata_response.get('sheets', [])
        if not sheets:
            return {
                "successful": False,
                "message": "No sheets found in the spreadsheet",
                "tables": [],
                "error": "No sheets available"
            }
        
        all_tables = []
        
        # Analyze each sheet for tables
        for sheet in sheets:
            sheet_props = sheet.get('properties', {})
            sheet_title = sheet_props.get('title', 'Unknown')
            sheet_id = sheet_props.get('sheetId', 0)
            
            # Get data from the sheet
            try:
                data_response = service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{sheet_title}'!A:ZZ"  # Get all columns
                ).execute()
            except Exception as e:
                # Skip sheets that can't be accessed
                continue
            
            values = data_response.get('values', [])
            if not values:
                continue
            
            # Find tables in this sheet
            sheet_tables = _detect_tables_in_sheet(
                values, 
                sheet_title, 
                sheet_id, 
                min_rows, 
                min_columns, 
                min_confidence
            )
            
            all_tables.extend(sheet_tables)
        
        # Sort tables by confidence score (highest first)
        all_tables.sort(key=lambda x: x['confidence'], reverse=True)
        
        return {
            "successful": True,
            "message": f"Found {len(all_tables)} table(s) across {len(sheets)} sheet(s)",
            "tables": all_tables,
            "total_tables": len(all_tables),
            "total_sheets": len(sheets),
            "analysis_parameters": {
                "min_rows": min_rows,
                "min_columns": min_columns,
                "min_confidence": min_confidence
            },
            "summary": {
                "high_confidence_tables": len([t for t in all_tables if t['confidence'] >= 0.8]),
                "medium_confidence_tables": len([t for t in all_tables if 0.5 <= t['confidence'] < 0.8]),
                "low_confidence_tables": len([t for t in all_tables if t['confidence'] < 0.5])
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error listing tables: {str(e)}",
            "tables": [],
            "error": str(e)
        }


def _detect_tables_in_sheet(values: list, sheet_title: str, sheet_id: int, min_rows: int, min_columns: int, min_confidence: float) -> list:
    """Detect tables within a sheet using heuristic analysis.
    
    Args:
        values: 2D array of cell values
        sheet_title: Name of the sheet
        sheet_id: ID of the sheet
        min_rows: Minimum rows required
        min_columns: Minimum columns required
        min_confidence: Minimum confidence threshold
        
    Returns:
        List of detected tables
    """
    tables = []
    
    if len(values) < min_rows + 1:  # +1 for header
        return tables
    
    # Strategy 1: Look for the main table (largest data region)
    main_table = _find_main_table(values, sheet_title, sheet_id, min_rows, min_columns)
    if main_table and main_table['confidence'] >= min_confidence:
        tables.append(main_table)
    
    # Strategy 2: Look for multiple smaller tables separated by empty rows
    smaller_tables = _find_smaller_tables(values, sheet_title, sheet_id, min_rows, min_columns)
    for table in smaller_tables:
        if table['confidence'] >= min_confidence and not _is_table_duplicate(table, tables):
            tables.append(table)
    
    return tables


def _is_table_duplicate(new_table: dict, existing_tables: list) -> bool:
    """Check if a table is a duplicate of an existing table."""
    for existing_table in existing_tables:
        # Check if tables overlap significantly
        if (new_table['start_row'] >= existing_table['start_row'] and 
            new_table['end_row'] <= existing_table['end_row'] and
            new_table['start_column'] == existing_table['start_column'] and
            new_table['end_column'] == existing_table['end_column']):
            return True
        
        # Check if tables have the same name and similar structure
        if (new_table['name'] == existing_table['name'] and
            abs(new_table['row_count'] - existing_table['row_count']) <= 2):
            return True
    
    return False


def _find_main_table(values: list, sheet_title: str, sheet_id: int, min_rows: int, min_columns: int) -> dict | None:
    """Find the main/largest table in the sheet."""
    if len(values) < 2:
        return None
    
    # Instead of finding one big table, look for the first well-formed table
    # This prevents detecting the entire sheet as one table
    for row_idx in range(len(values) - min_rows):
        if _is_likely_header_row(values[row_idx]):
            # Check if there's enough data below this header
            data_rows = 0
            table_end = row_idx + 1
            
            # Look for the end of this table (empty row or different structure)
            for check_row in range(row_idx + 1, len(values)):
                if _is_empty_row(values[check_row]):
                    # Found empty row, table ends here
                    table_end = check_row
                    break
                elif _is_likely_header_row(values[check_row]):
                    # Found another header, table ends before this
                    table_end = check_row
                    break
                else:
                    data_rows += 1
            
            if data_rows >= min_rows:
                table_data = values[row_idx:table_end]
                # Find the actual column boundary for this table
                max_cols = 0
                for row in table_data:
                    for col_idx, cell in enumerate(row):
                        if cell and str(cell).strip():
                            max_cols = max(max_cols, col_idx + 1)
                
                if max_cols >= min_columns:
                    # Trim the table data to actual columns
                    trimmed_table_data = []
                    for row in table_data:
                        trimmed_row = row[:max_cols]
                        # Pad if necessary
                        while len(trimmed_row) < max_cols:
                            trimmed_row.append('')
                        trimmed_table_data.append(trimmed_row)
                    
                    confidence = _calculate_table_confidence(trimmed_table_data)
                    table_name = _generate_table_name(trimmed_table_data[0] if trimmed_table_data else [], sheet_title)
                    
                    return {
                        "name": table_name,
                        "sheet_name": sheet_title,
                        "sheet_id": sheet_id,
                        "start_row": row_idx + 1,
                        "end_row": table_end,
                        "start_column": "A",
                        "end_column": chr(64 + max_cols),
                        "range": f"'{sheet_title}'!A{row_idx + 1}:{chr(64 + max_cols)}{table_end}",
                        "row_count": data_rows,
                        "column_count": max_cols,
                        "confidence": confidence,
                        "detection_method": "first_table",
                        "headers": trimmed_table_data[0] if trimmed_table_data else [],
                        "sample_data": trimmed_table_data[1:min(4, len(trimmed_table_data))] if len(trimmed_table_data) > 1 else []
                    }
    
    return None


def _find_smaller_tables(values: list, sheet_title: str, sheet_id: int, min_rows: int, min_columns: int) -> list:
    """Find smaller tables within the sheet."""
    tables = []
    
    # Look for tables separated by empty rows
    current_start = None
    for row_idx in range(len(values)):
        # Check if this row is empty or mostly empty
        is_empty = _is_empty_row(values[row_idx])
        
        if not is_empty and current_start is None:
            # Found start of a potential table
            current_start = row_idx
        elif is_empty and current_start is not None:
            # Found end of a potential table
            table_data = values[current_start:row_idx]
            if len(table_data) >= min_rows + 1 and any(len(row) >= min_columns for row in table_data):
                # Additional check: ensure this looks like a real table, not just data rows
                if _is_likely_real_table(table_data):
                    table = _create_table_from_data(table_data, sheet_title, sheet_id, current_start + 1, "empty_row_separation")
                    if table:
                        tables.append(table)
            current_start = None
    
    # Check the last potential table
    if current_start is not None:
        table_data = values[current_start:]
        if len(table_data) >= min_rows + 1 and any(len(row) >= min_columns for row in table_data):
            if _is_likely_real_table(table_data):
                table = _create_table_from_data(table_data, sheet_title, sheet_id, current_start + 1, "end_of_sheet")
                if table:
                    tables.append(table)
    
    return tables


def _is_empty_row(row: list) -> bool:
    """Check if a row is empty or mostly empty."""
    if not row:
        return True
    
    # Check if all cells are empty or just whitespace
    for cell in row:
        if cell and str(cell).strip():
            return False
    
    return True


def _is_likely_real_table(table_data: list) -> bool:
    """Check if the data looks like a real table rather than just data rows."""
    if not table_data or len(table_data) < 2:
        return False
    
    # Check if the first row looks like a header
    first_row = table_data[0]
    if not _is_likely_header_row(first_row):
        return False
    
    # Check if subsequent rows look like data (not headers)
    data_row_count = 0
    for row in table_data[1:]:
        if _looks_like_data_row(row):
            data_row_count += 1
    
    # At least 60% of non-header rows should look like data
    return data_row_count >= len(table_data[1:]) * 0.6


def _looks_like_data_row(row: list) -> bool:
    """Check if a row looks like data rather than a header."""
    if not row:
        return False
    
    # Data rows typically have mixed case, contain numbers, or are longer
    data_indicators = 0
    total_cells = len(row)
    
    for cell in row:
        cell_str = str(cell).strip()
        if not cell_str:
            continue
        
        # Check for data-like patterns
        if any(char.isdigit() for char in cell_str):
            data_indicators += 1
        if not cell_str.isupper() and not cell_str.istitle():
            data_indicators += 1
        if len(cell_str) > 20:  # Data can be longer than headers
            data_indicators += 1
        if any(char in cell_str for char in ['$', ',', '.', '-', '/']):  # Common data characters
            data_indicators += 1
    
    return data_indicators >= total_cells * 0.4  # 40% of cells show data characteristics


def _find_bounded_tables(values: list, sheet_title: str, sheet_id: int, min_rows: int, min_columns: int) -> list:
    """Find tables with clear boundaries (headers, formatting, etc.)."""
    tables = []
    
    # Look for tables with clear header patterns
    for row_idx in range(len(values) - min_rows):
        if _is_likely_header_row(values[row_idx]):
            # Check if there's data below
            data_rows = 0
            for check_row in range(row_idx + 1, min(row_idx + min_rows + 5, len(values))):
                if any(cell.strip() for cell in values[check_row] if cell):
                    data_rows += 1
            
            if data_rows >= min_rows:
                table_data = values[row_idx:row_idx + data_rows + 1]
                table = _create_table_from_data(table_data, sheet_title, sheet_id, row_idx + 1, "header_pattern")
                if table:
                    tables.append(table)
    
    return tables


def _create_table_from_data(table_data: list, sheet_title: str, sheet_id: int, start_row: int, detection_method: str) -> dict | None:
    """Create a table object from table data."""
    if not table_data or len(table_data) < 2:
        return None
    
    # Find the actual data boundary
    max_cols = max(len(row) for row in table_data)
    if max_cols == 0:
        return None
    
    # Trim empty columns
    while max_cols > 0:
        has_data = False
        for row in table_data:
            if len(row) > max_cols - 1 and row[max_cols - 1].strip():
                has_data = True
                break
        if has_data:
            break
        max_cols -= 1
    
    if max_cols == 0:
        return None
    
    # Extract headers and data
    headers = table_data[0][:max_cols]
    data_rows = table_data[1:]
    
    # Calculate confidence
    confidence = _calculate_table_confidence(table_data)
    
    # Generate table name
    table_name = _generate_table_name(headers, sheet_title)
    
    return {
        "name": table_name,
        "sheet_name": sheet_title,
        "sheet_id": sheet_id,
        "start_row": start_row,
        "end_row": start_row + len(data_rows),
        "start_column": "A",
        "end_column": chr(64 + max_cols),
        "range": f"'{sheet_title}'!A{start_row}:{chr(64 + max_cols)}{start_row + len(data_rows)}",
        "row_count": len(data_rows),
        "column_count": max_cols,
        "confidence": confidence,
        "detection_method": detection_method,
        "headers": headers,
        "sample_data": data_rows[:3] if data_rows else []
    }


def _is_likely_header_row(row: list) -> bool:
    """Check if a row is likely to be a header row."""
    if not row:
        return False
    
    # Check for common header patterns
    header_indicators = 0
    total_cells = len(row)
    non_empty_cells = 0
    
    for cell in row:
        cell_str = str(cell).strip()
        if not cell_str:
            continue
        
        non_empty_cells += 1
        
        # Check for header-like patterns
        if cell_str.isupper() or cell_str.istitle():
            header_indicators += 1
        if any(word in cell_str.lower() for word in ['name', 'id', 'date', 'total', 'amount', 'price', 'quantity', 'status', 'brand', 'manufacturer', 'category', 'features', 'style', 'material', 'item', 'electronic', 'shoes']):
            header_indicators += 1
        if len(cell_str) <= 25:  # Reasonable header length
            header_indicators += 1
        # Avoid treating data-like content as headers
        if any(char.isdigit() for char in cell_str):
            header_indicators -= 1  # Penalize numeric content
        if any(char in cell_str for char in ['$', ',', '.', '-', '/']):
            header_indicators -= 1  # Penalize data characters
    
    # Require at least 3 non-empty cells and 70% header indicators
    if non_empty_cells < 3:
        return False
    
    return header_indicators >= non_empty_cells * 0.7  # 70% of cells show header characteristics


def _calculate_table_confidence(table_data: list) -> float:
    """Calculate confidence score for a detected table."""
    if not table_data or len(table_data) < 2:
        return 0.0
    
    confidence = 0.0
    
    # Factor 1: Data consistency (0.3 points)
    if len(table_data) >= 3:
        col_counts = [len(row) for row in table_data]
        if len(set(col_counts)) <= 2:  # Consistent column count
            confidence += 0.3
    
    # Factor 2: Header quality (0.3 points)
    headers = table_data[0]
    header_quality = sum(1 for h in headers if str(h).strip()) / len(headers) if headers else 0
    confidence += header_quality * 0.3
    
    # Factor 3: Data density (0.2 points)
    total_cells = sum(len(row) for row in table_data)
    non_empty_cells = sum(1 for row in table_data for cell in row if str(cell).strip())
    if total_cells > 0:
        data_density = non_empty_cells / total_cells
        confidence += data_density * 0.2
    
    # Factor 4: Size factor (0.2 points)
    if len(table_data) >= 5:
        confidence += 0.2
    elif len(table_data) >= 3:
        confidence += 0.1
    
    return min(confidence, 1.0)


def _generate_table_name(headers: list, sheet_title: str) -> str:
    """Generate a meaningful table name from headers."""
    if not headers:
        return f"{sheet_title}_Table"
    
    # Try to find a meaningful header
    meaningful_headers = []
    for header in headers:
        header_str = str(header).strip()
        if header_str and len(header_str) <= 30:
            meaningful_headers.append(header_str)
    
    if meaningful_headers:
        # Use the first meaningful header
        base_name = meaningful_headers[0].replace(' ', '_').replace('-', '_')
        return f"{base_name}_Table"
    
    return f"{sheet_title}_Table"


@simple_mcp.tool()
async def lookup_spreadsheet_row(spreadsheet_id: str, query: str, range: str | None = None, case_sensitive: bool = False) -> dict:
    """Find the first row in a Google spreadsheet where a cell's entire content exactly matches the query string.
    
    Finds the first row in a google spreadsheet where a cell's entire content exactly matches the query string, 
    searching within a specified a1 notation range or the first sheet by default.
    
    Args:
        spreadsheet_id: Identifier of the Google Spreadsheet to search
        query: Exact text value to find; matches the entire content of a cell in a row
        range: A1 notation of the range to search (e.g., 'Sheet1!A1:D5', 'MySheet!A:Z', or 'Sheet1'). 
               Defaults to the non-empty part of the first sheet. For multiple sheets, include sheet name 
               (e.g., 'SheetName!A1:Z100')
        case_sensitive: If True, the query string search is case-sensitive
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "found_row": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "found_row": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not query:
        return {
            "successful": False,
            "message": "Error: Query string must be specified",
            "found_row": None,
            "error": "Missing query string"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Determine the range to search
        search_range = range
        if not search_range:
            # Get spreadsheet metadata to determine the first sheet
            try:
                metadata_response = service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id,
                    ranges=[],
                    includeGridData=False
                ).execute()
                
                sheets = metadata_response.get('sheets', [])
                if not sheets:
                    return {
                        "successful": False,
                        "message": "No sheets found in the spreadsheet",
                        "found_row": None,
                        "error": "No sheets available"
                    }
                
                # Use the first sheet
                first_sheet = sheets[0]
                sheet_title = first_sheet.get('properties', {}).get('title', 'Sheet1')
                search_range = f"'{sheet_title}'!A:ZZ"  # Search all columns
                
            except Exception as e:
                return {
                    "successful": False,
                    "message": f"Could not access spreadsheet metadata. Details: {str(e)}",
                    "found_row": None,
                    "error": str(e)
                }
        
        # Get the data from the specified range
        try:
            data_response = service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=search_range
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Could not retrieve data from range '{search_range}'. Details: {str(e)}",
                "found_row": None,
                "error": str(e)
            }
        
        values = data_response.get('values', [])
        if not values:
            return {
                "successful": False,
                "message": f"No data found in range '{search_range}'",
                "found_row": None,
                "error": "No data available"
            }
        
        # Parse the range to get sheet name and cell range
        sheet_name, cell_range = _parse_range(search_range)
        
        # Search for the query string
        found_row_info = None
        search_query = query if case_sensitive else query.lower()
        
        for row_idx, row in enumerate(values):
            for col_idx, cell_value in enumerate(row):
                cell_str = str(cell_value)
                compare_value = cell_str if case_sensitive else cell_str.lower()
                
                if compare_value == search_query:
                    # Convert 0-based index to 1-based row number
                    row_number = row_idx + 1
                    column_letter = chr(65 + col_idx)  # A, B, C, etc.
                    cell_address = f"{column_letter}{row_number}"
                    
                    found_row_info = {
                        "row_number": row_number,
                        "column_index": col_idx,
                        "column_letter": column_letter,
                        "cell_address": cell_address,
                        "cell_value": cell_str,
                        "sheet_name": sheet_name,
                        "range": search_range,
                        "query": query,
                        "case_sensitive": case_sensitive
                    }
                    break
            
            if found_row_info:
                break
        
        if found_row_info:
            return {
                "successful": True,
                "message": f"Found query '{query}' in cell {found_row_info['cell_address']} on row {found_row_info['row_number']}",
                "found_row": found_row_info,
                "search_range": search_range,
                "total_rows_searched": len(values),
                "total_cells_searched": sum(len(row) for row in values)
            }
        else:
            return {
                "successful": False,
                "message": f"Query '{query}' not found in range '{search_range}'",
                "found_row": None,
                "search_range": search_range,
                "total_rows_searched": len(values),
                "total_cells_searched": sum(len(row) for row in values),
                "error": "Query not found"
            }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error looking up spreadsheet row: {str(e)}",
            "found_row": None,
            "error": str(e)
        }


def _parse_range(range_str: str) -> tuple[str, str]:
    """Parse A1 notation range to extract sheet name and cell range.
    
    Args:
        range_str: A1 notation range (e.g., 'Sheet1!A1:B5', 'A1:B5')
        
    Returns:
        Tuple of (sheet_name, cell_range)
    """
    if '!' in range_str:
        sheet_name, cell_range = range_str.split('!', 1)
        # Remove quotes from sheet name if present
        sheet_name = sheet_name.strip().strip("'\"")
        return sheet_name, cell_range
    else:
        return "Sheet1", range_str


@simple_mcp.tool()
async def search_developer_metadata(spreadsheet_id: str, data_filters: list) -> dict:
    """Search for developer metadata in a spreadsheet.
    
    Tool to search for developer metadata in a spreadsheet. use when you need to find specific metadata entries based on filters.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet to search
        data_filters: List of data filters to apply for metadata search. Each filter can be:
            - A1 notation range string (e.g., "Sheet1!A1:C10")
            - DataFilter object with a1Range, gridRange, or developerMetadataLookup
            - Developer metadata lookup object with specific criteria
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "metadata_results": []
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "metadata_results": [],
            "error": "Missing spreadsheet ID"
        }
    
    if not data_filters:
        return {
            "successful": False,
            "message": "Error: Data filters must be specified",
            "metadata_results": [],
            "error": "Missing data filters"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Process data filters to ensure proper format
        processed_filters = []
        for filter_item in data_filters:
            if isinstance(filter_item, str):
                # Convert A1 notation to DataFilter object
                if '!' in filter_item:
                    processed_filters.append({
                        'a1Range': filter_item
                    })
                else:
                    # No sheet name specified, use first sheet
                    processed_filters.append({
                        'a1Range': f"Sheet1!{filter_item}"
                    })
            elif isinstance(filter_item, dict):
                # Already a proper DataFilter object
                processed_filters.append(filter_item)
            else:
                return {
                    "successful": False,
                    "message": f"Invalid data filter format: {filter_item}. Must be string (A1 notation) or dict (DataFilter object)",
                    "metadata_results": [],
                    "error": "Invalid filter format"
                }
        
        # Prepare the request body
        request_body = {
            'dataFilters': processed_filters,
            'includeGridData': False,
            'excludeTablesInBandedRanges': False
        }
        
        # Execute the search using getByDataFilter
        try:
            response = service.spreadsheets().getByDataFilter(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error searching developer metadata: {str(e)}",
                "metadata_results": [],
                "error": str(e)
            }
        
        # Extract metadata from the response
        metadata_results = []
        
        # Check for developer metadata in the response
        if 'sheets' in response:
            for sheet in response['sheets']:
                sheet_props = sheet.get('properties', {})
                sheet_title = sheet_props.get('title', 'Unknown')
                sheet_id = sheet_props.get('sheetId', 0)
                
                # Extract developer metadata from the sheet
                developer_metadata = sheet.get('developerMetadata', [])
                
                for metadata in developer_metadata:
                    metadata_info = {
                        "sheet_name": sheet_title,
                        "sheet_id": sheet_id,
                        "metadata_id": metadata.get('metadataId', 0),
                        "metadata_key": metadata.get('metadataKey', ''),
                        "metadata_value": metadata.get('metadataValue', ''),
                        "location": metadata.get('location', {}),
                        "visibility": metadata.get('visibility', 'DOCUMENT')
                    }
                    
                    # Add location details if available
                    location = metadata.get('location', {})
                    if 'dimensionRange' in location:
                        dim_range = location['dimensionRange']
                        metadata_info['location_details'] = {
                            "sheet_id": dim_range.get('sheetId', 0),
                            "dimension": dim_range.get('dimension', ''),
                            "start_index": dim_range.get('startIndex', 0),
                            "end_index": dim_range.get('endIndex', 0)
                        }
                    
                    metadata_results.append(metadata_info)
        
        # Also check for document-level metadata
        if 'developerMetadata' in response:
            for metadata in response['developerMetadata']:
                metadata_info = {
                    "sheet_name": "Document Level",
                    "sheet_id": None,
                    "metadata_id": metadata.get('metadataId', 0),
                    "metadata_key": metadata.get('metadataKey', ''),
                    "metadata_value": metadata.get('metadataValue', ''),
                    "location": metadata.get('location', {}),
                    "visibility": metadata.get('visibility', 'DOCUMENT')
                }
                
                # Add location details if available
                location = metadata.get('location', {})
                if 'dimensionRange' in location:
                    dim_range = location['dimensionRange']
                    metadata_info['location_details'] = {
                        "sheet_id": dim_range.get('sheetId', 0),
                        "dimension": dim_range.get('dimension', ''),
                        "start_index": dim_range.get('startIndex', 0),
                        "end_index": dim_range.get('endIndex', 0)
                    }
                
                metadata_results.append(metadata_info)
        
        # Get spreadsheet info for context
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
            
            spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', 'Unknown')
        except Exception:
            spreadsheet_title = "Unknown"
        
        return {
            "successful": True,
            "message": f"Found {len(metadata_results)} developer metadata entries in spreadsheet '{spreadsheet_title}'",
            "metadata_results": metadata_results,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_title": spreadsheet_title,
            "total_metadata_entries": len(metadata_results),
            "search_filters": processed_filters,
            "summary": {
                "sheet_level_metadata": len([m for m in metadata_results if m['sheet_name'] != 'Document Level']),
                "document_level_metadata": len([m for m in metadata_results if m['sheet_name'] == 'Document Level']),
                "unique_keys": len(set(m['metadata_key'] for m in metadata_results if m['metadata_key'])),
                "unique_sheets": len(set(m['sheet_name'] for m in metadata_results))
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error searching developer metadata: {str(e)}",
            "metadata_results": [],
            "error": str(e)
        }


@simple_mcp.tool()
async def search_spreadsheets(query: str | None = None, max_results: int = 10, order_by: str = "modifiedTime desc", shared_with_me: bool = False, starred_only: bool = False, include_trashed: bool = False, created_after: str | None = None, modified_after: str | None = None) -> dict:
    """Search for Google spreadsheets using various filters including name, content, date ranges, and more.
    
    Search for google spreadsheets using various filters including name, content, date ranges, and more.
    
    Args:
        query: Search query to filter spreadsheets. Can search by name (name contains 'budget'), 
               full text content (fullText contains 'sales'), or use complex queries with operators 
               like 'and', 'or', 'not'. Leave empty to get all spreadsheets.
        max_results: Maximum number of spreadsheets to return (1-1000). Defaults to 10.
        order_by: Order results by field. Common options: 'modifiedTime desc', 'modifiedTime asc', 
                  'name', 'createdTime desc'
        shared_with_me: Whether to return only spreadsheets shared with the current user. Defaults to False.
        starred_only: Whether to return only starred spreadsheets. Defaults to False.
        include_trashed: Whether to include spreadsheets in trash. Defaults to False.
        created_after: Return spreadsheets created after this date. Use RFC 3339 format like '2024-01-01T00:00:00Z'.
        modified_after: Return spreadsheets modified after this date. Use RFC 3339 format like '2024-01-01T00:00:00Z'.
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheets": []
        }
    
    # Validate parameters
    if max_results < 1 or max_results > 1000:
        return {
            "successful": False,
            "message": "Error: max_results must be between 1 and 1000",
            "spreadsheets": [],
            "error": "Invalid max_results value"
        }
    
    try:
        # Build the Drive service
        service = build('drive', 'v3', credentials=creds)
        
        # Build the query string
        query_parts = []
        
        # File type filter (only spreadsheets)
        query_parts.append("mimeType='application/vnd.google-apps.spreadsheet'")
        
        # Add search query if provided
        if query:
            # Convert common search patterns to Google Drive API syntax
            processed_query = query
            
            # Handle simple string queries (convert to name contains)
            if not any(op in processed_query.lower() for op in ["name contains", "fulltext contains", "name =", "createdtime", "modifiedtime", "sharedwithme", "starred", "trashed", "owners", "in parents"]):
                # If it's just a simple string, convert it to a name contains query
                processed_query = f"name contains '{processed_query}'"
            
            # Handle common search patterns - Google Drive API is case-sensitive
            if "name contains" in processed_query.lower():
                # Convert "name contains 'value'" to "name contains 'value'" (lowercase)
                processed_query = processed_query.lower().replace("name contains", "name contains")
            elif "fulltext contains" in processed_query.lower():
                # Convert "fulltext contains 'value'" to "fullText contains 'value'" (correct case)
                processed_query = processed_query.replace("fulltext contains", "fullText contains")
            elif "name =" in processed_query:
                # Convert "name = 'value'" to "name = 'value'" (lowercase)
                processed_query = processed_query.lower().replace("name =", "name =")
            
            # Extract only valid Google Drive API query parts
            # Remove natural language parts that aren't supported
            valid_operators = [
                "name contains", "name =", "fullText contains", 
                "createdTime >", "createdTime <", "modifiedTime >", "modifiedTime <",
                "sharedWithMe", "starred", "trashed", "owners", "in parents"
            ]
            
            # Split the query and keep only valid parts
            query_words = processed_query.split()
            valid_parts = []
            i = 0
            while i < len(query_words):
                # Check for multi-word operators
                if i + 1 < len(query_words):
                    two_word_op = f"{query_words[i]} {query_words[i+1]}"
                    if two_word_op in valid_operators:
                        valid_parts.extend([query_words[i], query_words[i+1]])
                        i += 2
                        continue
                
                # Check for single-word operators
                if query_words[i] in ["sharedWithMe", "starred", "trashed"]:
                    valid_parts.append(query_words[i])
                elif query_words[i] in ["and", "or", "not", "(", ")", "=", ">", "<", "'", '"']:
                    # Keep logical operators and quotes
                    valid_parts.append(query_words[i])
                elif query_words[i].startswith("'") or query_words[i].endswith("'"):
                    # Keep quoted strings
                    valid_parts.append(query_words[i])
                elif query_words[i].startswith('"') or query_words[i].endswith('"'):
                    # Keep quoted strings
                    valid_parts.append(query_words[i])
                else:
                    # Skip unrecognized terms
                    pass
                i += 1
            
            # Reconstruct the valid query
            if valid_parts:
                processed_query = " ".join(valid_parts)
                
                # Clean up incomplete logical expressions
                processed_query = processed_query.strip()
                
                # Remove trailing logical operators
                while processed_query.endswith((' and', ' or', ' not')):
                    processed_query = processed_query[:-4] if processed_query.endswith(' and') else processed_query[:-3]
                
                # Remove leading logical operators
                while processed_query.startswith(('and ', 'or ', 'not ')):
                    processed_query = processed_query[4:] if processed_query.startswith('and ') else processed_query[3:]
                
                # Remove empty parentheses
                processed_query = processed_query.replace('()', '').replace('( )', '')
                
                # Only add the query if it's not empty and contains valid content
                if processed_query and not processed_query.isspace():
                    # Add the processed query
                    query_parts.append(f"({processed_query})")
            else:
                # If no valid parts found, skip the query
                pass
        
        # Add date filters
        if created_after:
            query_parts.append(f"createdTime > '{created_after}'")
        
        if modified_after:
            query_parts.append(f"modifiedTime > '{modified_after}'")
        
        # Add visibility filters
        if shared_with_me:
            query_parts.append("sharedWithMe=true")
        
        if starred_only:
            query_parts.append("starred=true")
        
        # Add trashed filter
        if include_trashed:
            query_parts.append("trashed=true")
        else:
            query_parts.append("trashed=false")
        
        # Combine all query parts
        final_query = " and ".join(query_parts)
        
        # Prepare the request
        request_params = {
            'q': final_query,
            'pageSize': max_results,
            'orderBy': order_by,
            'fields': "nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size, owners, shared, starred, trashed, parents, webViewLink, webContentLink)"
        }
        
        # Add page token if provided (for pagination)
        if hasattr(search_spreadsheets, 'page_token') and search_spreadsheets.page_token:
            request_params['pageToken'] = search_spreadsheets.page_token
        
        request = service.files().list(**request_params)
        
        # Execute the search
        try:
            response = request.execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error searching spreadsheets: {str(e)}",
                "spreadsheets": [],
                "error": str(e)
            }
        
        # Check if response is valid
        if not isinstance(response, dict):
            return {
                "successful": False,
                "message": f"Invalid response format from Google Drive API. Response type: {type(response)}",
                "spreadsheets": [],
                "error": f"Invalid response format: {type(response)}"
            }
        
        files = response.get('files', [])
        next_page_token = response.get('nextPageToken')
        
        # Debug: Log the response structure
        if not files:
            return {
                "successful": True,
                "message": "No spreadsheets found matching the search criteria",
                "spreadsheets": [],
                "total_results": 0,
                "search_query": final_query,
                "pagination": {
                    "has_more": False,
                    "next_page_token": None,
                    "total_estimated": 0
                },
                "search_parameters": {
                    "query": query,
                    "max_results": max_results,
                    "order_by": order_by,
                    "shared_with_me": shared_with_me,
                    "starred_only": starred_only,
                    "include_trashed": include_trashed,
                    "created_after": created_after,
                    "modified_after": modified_after
                },
                "summary": {
                    "total_spreadsheets": 0,
                    "shared_spreadsheets": 0,
                    "starred_spreadsheets": 0,
                    "trashed_spreadsheets": 0,
                    "owned_by_me": 0
                }
            }
        
        # Process the results
        spreadsheets = []
        try:
            for file in files:
                if not isinstance(file, dict):
                    continue
                    
                spreadsheet_info = {
                    "id": file.get('id', ''),
                    "name": file.get('name', ''),
                    "mime_type": file.get('mimeType', ''),
                    "created_time": file.get('createdTime', ''),
                    "modified_time": file.get('modifiedTime', ''),
                    "size": file.get('size', '0'),
                    "shared": file.get('shared', False),
                    "starred": file.get('starred', False),
                    "trashed": file.get('trashed', False),
                    "web_view_link": file.get('webViewLink', ''),
                    "web_content_link": file.get('webContentLink', ''),
                    "owners": [owner.get('displayName', '') for owner in file.get('owners', []) if isinstance(owner, dict)],
                    "parent_folders": [parent.get('id', '') for parent in file.get('parents', []) if isinstance(parent, dict)]
                }
                
                # Add human-readable dates
                if spreadsheet_info['created_time']:
                    try:
                        from datetime import datetime
                        created_dt = datetime.fromisoformat(spreadsheet_info['created_time'].replace('Z', '+00:00'))
                        spreadsheet_info['created_date'] = created_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        spreadsheet_info['created_date'] = spreadsheet_info['created_time']
                
                if spreadsheet_info['modified_time']:
                    try:
                        from datetime import datetime
                        modified_dt = datetime.fromisoformat(spreadsheet_info['modified_time'].replace('Z', '+00:00'))
                        spreadsheet_info['modified_date'] = modified_dt.strftime('%Y-%m-%d %H:%M:%S')
                    except:
                        spreadsheet_info['modified_date'] = spreadsheet_info['modified_time']
                
                # Add size in human-readable format
                if spreadsheet_info['size'] != '0':
                    try:
                        size_bytes = int(spreadsheet_info['size'])
                        if size_bytes < 1024:
                            spreadsheet_info['size_formatted'] = f"{size_bytes} B"
                        elif size_bytes < 1024 * 1024:
                            spreadsheet_info['size_formatted'] = f"{size_bytes / 1024:.1f} KB"
                        else:
                            spreadsheet_info['size_formatted'] = f"{size_bytes / (1024 * 1024):.1f} MB"
                    except:
                        spreadsheet_info['size_formatted'] = spreadsheet_info['size']
                else:
                    spreadsheet_info['size_formatted'] = "0 B"
                
                spreadsheets.append(spreadsheet_info)
        
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error processing spreadsheet data: {str(e)}",
                "spreadsheets": [],
                "error": str(e)
            }
        
        # Build pagination info
        pagination_info = {
            "has_more": next_page_token is not None,
            "next_page_token": next_page_token,
            "total_estimated": len(spreadsheets) + (len(spreadsheets) if next_page_token else 0)  # Rough estimate
        }
        
        return {
            "successful": True,
            "message": f"Found {len(spreadsheets)} spreadsheet(s) matching the search criteria (showing up to {max_results})",
            "spreadsheets": spreadsheets,
            "total_results": len(spreadsheets),
            "search_query": final_query,
            "pagination": pagination_info,
            "search_parameters": {
                "query": query,
                "max_results": max_results,
                "order_by": order_by,
                "shared_with_me": shared_with_me,
                "starred_only": starred_only,
                "include_trashed": include_trashed,
                "created_after": created_after,
                "modified_after": modified_after
            },
            "summary": {
                "total_spreadsheets": len(spreadsheets),
                "shared_spreadsheets": len([s for s in spreadsheets if s['shared']]),
                "starred_spreadsheets": len([s for s in spreadsheets if s['starred']]),
                "trashed_spreadsheets": len([s for s in spreadsheets if s['trashed']]),
                "owned_by_me": len([s for s in spreadsheets if s['owners'] and len(s['owners']) > 0])
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error searching spreadsheets: {str(e)}",
            "spreadsheets": [],
            "error": str(e)
        }


@simple_mcp.tool()
async def set_basic_filter(spreadsheet_id: str, filter: dict) -> dict:
    """Set a basic filter on a sheet in a Google spreadsheet.
    
    Tool to set a basic filter on a sheet in a google spreadsheet. use when you need to filter or sort data within a specific range on a sheet.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet
        filter: The filter to set. Should contain:
            - range: Object containing:
                - sheet_id: The ID of the sheet where the filter will be applied
                - start_row_index: The start row (0-based) of the range
                - end_row_index: The end row (0-based, exclusive) of the range
                - start_column_index: The start column (0-based) of the range
                - end_column_index: The end column (0-based, exclusive) of the range
            - criteria: Optional filter criteria for specific columns
            - sort_specs: Optional sort specifications
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "filter": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "filter": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not filter:
        return {
            "successful": False,
            "message": "Error: Filter object must be specified",
            "filter": None,
            "error": "Missing filter object"
        }
    
    # Validate filter structure
    if 'range' not in filter:
        return {
            "successful": False,
            "message": "Error: Filter must contain 'range' object",
            "filter": None,
            "error": "Missing range in filter"
        }
    
    range_obj = filter['range']
    required_fields = ['sheet_id']
    for field in required_fields:
        if field not in range_obj:
            return {
                "successful": False,
                "message": f"Error: Range object must contain '{field}' field",
                "filter": None,
                "error": f"Missing {field} in range object"
            }
    
    # Validate range indices if provided
    if 'start_row_index' in range_obj and 'end_row_index' in range_obj:
        if range_obj['start_row_index'] < 0 or range_obj['end_row_index'] <= range_obj['start_row_index']:
            return {
                "successful": False,
                "message": "Error: start_row_index must be >= 0 and end_row_index must be > start_row_index",
                "filter": None,
                "error": "Invalid row indices"
            }
    
    if 'start_column_index' in range_obj and 'end_column_index' in range_obj:
        if range_obj['start_column_index'] < 0 or range_obj['end_column_index'] <= range_obj['start_column_index']:
            return {
                "successful": False,
                "message": "Error: start_column_index must be >= 0 and end_column_index must be > start_column_index",
                "filter": None,
                "error": "Invalid column indices"
            }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Prepare the request body
        request_body = {
            'requests': [{
                'setBasicFilter': {
                    'filter': filter
                }
            }]
        }
        
        # Execute the batch update
        try:
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error setting basic filter: {str(e)}",
                "filter": None,
                "error": str(e)
            }
        
        # Extract response information
        result = {
            "successful": True,
            "message": f"Successfully set basic filter on sheet {range_obj['sheet_id']}",
            "filter": filter,
            "spreadsheet_id": spreadsheet_id,
            "replies": response.get('replies', []),
            "updated_range": response.get('updatedRange'),
            "updated_rows": response.get('updatedRows'),
            "updated_columns": response.get('updatedColumns'),
            "updated_cells": response.get('updatedCells')
        }
        
        # Add filter details
        result['filter_details'] = {
            "sheet_id": range_obj['sheet_id'],
            "start_row_index": range_obj.get('start_row_index', 0),
            "end_row_index": range_obj.get('end_row_index', None),
            "start_column_index": range_obj.get('start_column_index', 0),
            "end_column_index": range_obj.get('end_column_index', None),
            "has_criteria": 'criteria' in filter,
            "has_sort_specs": 'sortSpecs' in filter
        }
        
        # Add user-friendly description
        if range_obj.get('end_row_index') and range_obj.get('end_column_index'):
            start_row = range_obj['start_row_index'] + 1  # Convert to 1-based
            end_row = range_obj['end_row_index']
            start_col = chr(65 + range_obj['start_column_index'])  # Convert to letter
            end_col = chr(64 + range_obj['end_column_index'])
            
            result['description'] = f"Filter applied to range {start_col}{start_row}:{end_col}{end_row}"
        else:
            result['description'] = f"Filter applied to entire sheet {range_obj['sheet_id']}"
        
        # Add criteria summary if present
        if 'criteria' in filter:
            criteria_count = len(filter['criteria'])
            result['criteria_summary'] = {
                "total_criteria": criteria_count,
                "columns_with_filters": list(filter['criteria'].keys())
            }
        
        # Add sort summary if present
        if 'sortSpecs' in filter:
            sort_count = len(filter['sortSpecs'])
            result['sort_summary'] = {
                "total_sort_specs": sort_count,
                "sort_columns": [spec.get('dimensionIndex', 'unknown') for spec in filter['sortSpecs']]
            }
        
        return result
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error setting basic filter: {str(e)}",
            "filter": None,
            "error": str(e)
        }


@simple_mcp.tool()
async def create_sheet_from_json(title: str, sheet_name: str, sheet_json: list) -> dict:
    """Create a new Google spreadsheet and populate its first worksheet from JSON data.
    
    Creates a new google spreadsheet and populates its first worksheet from `sheet json`, 
    which must be non-empty as its first item's keys establish the headers.
    
    Args:
        title: The title for the new Google Sheet (will be the name of the file in Google Drive)
        sheet_name: The name for the first worksheet/tab
        sheet_json: Array of objects where the first item's keys establish the headers. 
                   Each object should have the same keys as the first item.
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheet_id": None
        }
    
    if not title:
        return {
            "successful": False,
            "message": "Error: Title must be specified",
            "spreadsheet_id": None,
            "error": "Missing title"
        }
    
    if not sheet_name:
        return {
            "successful": False,
            "message": "Error: Sheet name must be specified",
            "spreadsheet_id": None,
            "error": "Missing sheet name"
        }
    
    if not sheet_json:
        return {
            "successful": False,
            "message": "Error: Sheet JSON must be specified and non-empty",
            "spreadsheet_id": None,
            "error": "Missing or empty sheet JSON"
        }
    
    if not isinstance(sheet_json, list) or len(sheet_json) == 0:
        return {
            "successful": False,
            "message": "Error: Sheet JSON must be a non-empty array",
            "spreadsheet_id": None,
            "error": "Invalid sheet JSON format"
        }
    
    # Validate that all items are dictionaries
    for i, item in enumerate(sheet_json):
        if not isinstance(item, dict):
            return {
                "successful": False,
                "message": f"Error: Item at index {i} must be a dictionary",
                "spreadsheet_id": None,
                "error": f"Invalid item type at index {i}"
            }
    
    # Get headers from the first item
    first_item = sheet_json[0]
    headers = list(first_item.keys())
    
    if not headers:
        return {
            "successful": False,
            "message": "Error: First item must have at least one key to establish headers",
            "spreadsheet_id": None,
            "error": "No headers found in first item"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Create the spreadsheet
        try:
            spreadsheet_response = service.spreadsheets().create(
                body={
                    'properties': {
                        'title': title
                    },
                    'sheets': [
                        {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    ]
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error creating spreadsheet: {str(e)}",
                "spreadsheet_id": None,
                "error": str(e)
            }
        
        spreadsheet_id = spreadsheet_response['spreadsheetId']
        
        # Prepare the data for the sheet
        # Start with headers
        sheet_data = [headers]
        
        # Add data rows
        for item in sheet_json:
            row = []
            for header in headers:
                # Get value for this header, convert to string, handle None
                value = item.get(header, '')
                if value is None:
                    value = ''
                row.append(str(value))
            sheet_data.append(row)
        
        # Update the sheet with data
        try:
            update_response = service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!A1",
                valueInputOption='USER_ENTERED',
                body={
                    'values': sheet_data
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error populating sheet with data: {str(e)}",
                "spreadsheet_id": spreadsheet_id,
                "error": str(e)
            }
        
        # Get the updated range
        updated_range = update_response.get('updatedRange', '')
        
        # Format the headers (make them bold)
        try:
            format_response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    'requests': [
                        {
                            'repeatCell': {
                                'range': {
                                    'sheetId': spreadsheet_response['sheets'][0]['properties']['sheetId'],
                                    'startRowIndex': 0,
                                    'endRowIndex': 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'textFormat': {
                                            'bold': True
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.textFormat.bold'
                            }
                        }
                    ]
                }
            ).execute()
        except Exception as e:
            # Non-critical error, continue without formatting
            pass
        
        # Get spreadsheet info
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
            
            spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', title)
            sheet_count = len(spreadsheet_info.get('sheets', []))
        except Exception:
            spreadsheet_title = title
            sheet_count = 1
        
        return {
            "successful": True,
            "message": f"Successfully created spreadsheet '{spreadsheet_title}' with {len(sheet_data) - 1} data rows",
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_title": spreadsheet_title,
            "sheet_name": sheet_name,
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit",
            "data_summary": {
                "total_rows": len(sheet_data),
                "data_rows": len(sheet_data) - 1,
                "total_columns": len(headers),
                "headers": headers,
                "updated_range": updated_range
            },
            "sheet_count": sheet_count,
            "headers_formatted": True
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error creating sheet from JSON: {str(e)}",
            "spreadsheet_id": None,
            "error": str(e)
        }


@simple_mcp.tool()
async def copy_sheet_to_another_spreadsheet(spreadsheet_id: str, sheet_id: int, destination_spreadsheet_id: str) -> dict:
    """Copy a single sheet from a spreadsheet to another spreadsheet.
    
    Tool to copy a single sheet from a spreadsheet to another spreadsheet. 
    use when you need to duplicate a sheet into a different spreadsheet.
    
    Args:
        spreadsheet_id: The ID of the source spreadsheet containing the sheet to copy
        sheet_id: The ID of the sheet to copy (integer)
        destination_spreadsheet_id: The ID of the destination spreadsheet where the sheet will be copied
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "source_spreadsheet_id": None,
            "destination_spreadsheet_id": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Source spreadsheet ID must be specified",
            "source_spreadsheet_id": None,
            "destination_spreadsheet_id": None,
            "error": "Missing source spreadsheet ID"
        }
    
    if not destination_spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Destination spreadsheet ID must be specified",
            "source_spreadsheet_id": None,
            "destination_spreadsheet_id": None,
            "error": "Missing destination spreadsheet ID"
        }
    
    if sheet_id is None:
        return {
            "successful": False,
            "message": "Error: Sheet ID must be specified",
            "source_spreadsheet_id": None,
            "destination_spreadsheet_id": None,
            "error": "Missing sheet ID"
        }
    
    if not isinstance(sheet_id, int):
        return {
            "successful": False,
            "message": "Error: Sheet ID must be an integer",
            "source_spreadsheet_id": None,
            "destination_spreadsheet_id": None,
            "error": "Invalid sheet ID type"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # First, get information about the source sheet to validate it exists
        try:
            source_spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing source spreadsheet: {str(e)}",
                "source_spreadsheet_id": spreadsheet_id,
                "destination_spreadsheet_id": destination_spreadsheet_id,
                "error": str(e)
            }
        
        # Find the source sheet
        source_sheet = None
        source_sheet_title = None
        for sheet in source_spreadsheet_info.get('sheets', []):
            if sheet['properties']['sheetId'] == sheet_id:
                source_sheet = sheet
                source_sheet_title = sheet['properties']['title']
                break
        
        if not source_sheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" 
                              for sheet in source_spreadsheet_info.get('sheets', [])]
            return {
                "successful": False,
                "message": f"Sheet with ID {sheet_id} not found in source spreadsheet",
                "source_spreadsheet_id": spreadsheet_id,
                "destination_spreadsheet_id": destination_spreadsheet_id,
                "available_sheets": available_sheets,
                "error": f"Sheet ID {sheet_id} not found"
            }
        
        # Validate destination spreadsheet exists
        try:
            destination_spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=destination_spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing destination spreadsheet: {str(e)}",
                "source_spreadsheet_id": spreadsheet_id,
                "destination_spreadsheet_id": destination_spreadsheet_id,
                "error": str(e)
            }
        
        # Check if destination spreadsheet already has a sheet with the same name
        destination_sheet_names = [sheet['properties']['title'] 
                                 for sheet in destination_spreadsheet_info.get('sheets', [])]
        
        # Generate a unique name if there's a conflict
        new_sheet_title = source_sheet_title
        counter = 1
        while new_sheet_title in destination_sheet_names:
            new_sheet_title = f"{source_sheet_title} (Copy {counter})"
            counter += 1
        
        # Copy the sheet to the destination spreadsheet
        try:
            copy_response = service.spreadsheets().sheets().copyTo(
                spreadsheetId=spreadsheet_id,
                sheetId=sheet_id,
                body={
                    'destinationSpreadsheetId': destination_spreadsheet_id
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error copying sheet: {str(e)}",
                "source_spreadsheet_id": spreadsheet_id,
                "destination_spreadsheet_id": destination_spreadsheet_id,
                "error": str(e)
            }
        
        # Get the new sheet ID from the response
        new_sheet_id = copy_response['sheetId']
        
        # Update the sheet title if it was changed due to naming conflict
        if new_sheet_title != source_sheet_title:
            try:
                service.spreadsheets().batchUpdate(
                    spreadsheetId=destination_spreadsheet_id,
                    body={
                        'requests': [
                            {
                                'updateSheetProperties': {
                                    'properties': {
                                        'sheetId': new_sheet_id,
                                        'title': new_sheet_title
                                    },
                                    'fields': 'title'
                                }
                            }
                        ]
                    }
                ).execute()
            except Exception as e:
                # Non-critical error, continue without renaming
                pass
        
        # Get updated destination spreadsheet info
        try:
            updated_destination_info = service.spreadsheets().get(
                spreadsheetId=destination_spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
            
            destination_title = updated_destination_info.get('properties', {}).get('title', 'Unknown')
            total_sheets = len(updated_destination_info.get('sheets', []))
        except Exception:
            destination_title = "Unknown"
            total_sheets = len(destination_sheet_names) + 1
        
        return {
            "successful": True,
            "message": f"Successfully copied sheet '{source_sheet_title}' to spreadsheet '{destination_title}'",
            "source_spreadsheet_id": spreadsheet_id,
            "source_spreadsheet_title": source_spreadsheet_info.get('properties', {}).get('title', 'Unknown'),
            "source_sheet_id": sheet_id,
            "source_sheet_title": source_sheet_title,
            "destination_spreadsheet_id": destination_spreadsheet_id,
            "destination_spreadsheet_title": destination_title,
            "new_sheet_id": new_sheet_id,
            "new_sheet_title": new_sheet_title,
            "destination_web_view_link": f"https://docs.google.com/spreadsheets/d/{destination_spreadsheet_id}/edit",
            "total_sheets_in_destination": total_sheets,
            "title_changed": new_sheet_title != source_sheet_title,
            "copy_details": {
                "original_sheet_id": sheet_id,
                "new_sheet_id": new_sheet_id,
                "sheet_properties": copy_response.get('properties', {})
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error copying sheet to another spreadsheet: {str(e)}",
            "source_spreadsheet_id": spreadsheet_id,
            "destination_spreadsheet_id": destination_spreadsheet_id,
            "error": str(e)
        }


@simple_mcp.tool()
async def append_values_to_spreadsheet(spreadsheetId: str, range: str, values: list, valueInputOption: str = "USER_ENTERED", insertDataOption: str = "INSERT_ROWS", majorDimension: str = "ROWS", includeValuesInResponse: bool = False, responseValueRenderOption: str = "UNFORMATTED_VALUE", responseDateTimeRenderOption: str = "SERIAL_NUMBER") -> dict:
    """Append values to a spreadsheet.
    
    Tool to append values to a spreadsheet. use when you need to add new data 
    to the end of an existing table in a google sheet.
    
    Args:
        spreadsheetId: The ID of the spreadsheet to append data to
        range: The A1 notation of a range to search for a logical table of data. 
               Values will be appended after the last row of the table.
        values: The values to append to the spreadsheet
        valueInputOption: How the input data should be interpreted
        insertDataOption: How the input data should be inserted
        majorDimension: The major dimension of the values
        includeValuesInResponse: Determines if the update response should include the values
        responseValueRenderOption: Determines how values in the response should be rendered
        responseDateTimeRenderOption: Determines how dates, times, and durations in the response should be rendered
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheetId": None
        }
    
    if not spreadsheetId:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheetId": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not range:
        return {
            "successful": False,
            "message": "Error: Range must be specified",
            "spreadsheetId": None,
            "error": "Missing range"
        }
    
    if not values:
        return {
            "successful": False,
            "message": "Error: Values must be specified",
            "spreadsheetId": None,
            "error": "Missing values"
        }
    
    if not isinstance(values, list):
        return {
            "successful": False,
            "message": "Error: Values must be an array",
            "spreadsheetId": None,
            "error": "Invalid values format"
        }
    
    # Validate valueInputOption
    valid_value_input_options = ["RAW", "USER_ENTERED"]
    if valueInputOption not in valid_value_input_options:
        return {
            "successful": False,
            "message": f"Error: valueInputOption must be one of {valid_value_input_options}",
            "spreadsheetId": None,
            "error": f"Invalid valueInputOption: {valueInputOption}"
        }
    
    # Validate insertDataOption
    valid_insert_data_options = ["OVERWRITE", "INSERT_ROWS"]
    if insertDataOption not in valid_insert_data_options:
        return {
            "successful": False,
            "message": f"Error: insertDataOption must be one of {valid_insert_data_options}",
            "spreadsheetId": None,
            "error": f"Invalid insertDataOption: {insertDataOption}"
        }
    
    # Validate majorDimension
    valid_major_dimensions = ["ROWS", "COLUMNS"]
    if majorDimension not in valid_major_dimensions:
        return {
            "successful": False,
            "message": f"Error: majorDimension must be one of {valid_major_dimensions}",
            "spreadsheetId": None,
            "error": f"Invalid majorDimension: {majorDimension}"
        }
    
    # Validate responseValueRenderOption
    valid_response_value_render_options = ["FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"]
    if responseValueRenderOption not in valid_response_value_render_options:
        return {
            "successful": False,
            "message": f"Error: responseValueRenderOption must be one of {valid_response_value_render_options}",
            "spreadsheetId": None,
            "error": f"Invalid responseValueRenderOption: {responseValueRenderOption}"
        }
    
    # Validate responseDateTimeRenderOption
    valid_response_datetime_render_options = ["SERIAL_NUMBER", "FORMATTED_STRING"]
    if responseDateTimeRenderOption not in valid_response_datetime_render_options:
        return {
            "successful": False,
            "message": f"Error: responseDateTimeRenderOption must be one of {valid_response_datetime_render_options}",
            "spreadsheetId": None,
            "error": f"Invalid responseDateTimeRenderOption: {responseDateTimeRenderOption}"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Validate spreadsheet exists
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing spreadsheet: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Prepare the request body
        request_body = {
            'values': values
        }
        
        # Append values to the spreadsheet
        try:
            response = service.spreadsheets().values().append(
                spreadsheetId=spreadsheetId,
                range=range,
                valueInputOption=valueInputOption,
                insertDataOption=insertDataOption,
                body=request_body,
                includeValuesInResponse=includeValuesInResponse,
                responseValueRenderOption=responseValueRenderOption,
                responseDateTimeRenderOption=responseDateTimeRenderOption
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error appending values: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Extract response data
        updates = response.get('updates', {})
        updated_range = updates.get('updatedRange', '')
        updated_rows = updates.get('updatedRows', 0)
        updated_columns = updates.get('updatedColumns', 0)
        updated_cells = updates.get('updatedCells', 0)
        
        # Get appended values if requested
        appended_values = None
        if includeValuesInResponse and 'updatedData' in updates:
            appended_values = updates['updatedData'].get('values', [])
        
        # Get spreadsheet title
        spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', 'Unknown')
        
        return {
            "successful": True,
            "message": f"Successfully appended {len(values)} row(s) to spreadsheet '{spreadsheet_title}'",
            "spreadsheetId": spreadsheetId,
            "spreadsheet_title": spreadsheet_title,
            "range": range,
            "updated_range": updated_range,
            "updated_rows": updated_rows,
            "updated_columns": updated_columns,
            "updated_cells": updated_cells,
            "appended_values": appended_values,
            "valueInputOption": valueInputOption,
            "insertDataOption": insertDataOption,
            "majorDimension": majorDimension,
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheetId}/edit",
            "append_details": {
                "total_rows_appended": len(values),
                "input_values": values,
                "response_metadata": {
                    "responseValueRenderOption": responseValueRenderOption,
                    "responseDateTimeRenderOption": responseDateTimeRenderOption
                }
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error appending values to spreadsheet: {str(e)}",
            "spreadsheetId": spreadsheetId,
            "error": str(e)
        }


@simple_mcp.tool()
async def batch_clear_spreadsheet_values(spreadsheet_id: str, ranges: list) -> dict:
    """Clear one or more ranges of values from a spreadsheet.
    
    Tool to clear one or more ranges of values from a spreadsheet. use when you need 
    to remove data from specific cells or ranges while keeping formatting and other properties intact.
    
    Args:
        spreadsheet_id: The ID of the spreadsheet to clear values from
        ranges: Array of A1 notation ranges to clear (e.g., ["Sheet1!A1:B5", "Sheet2!C3:D8"])
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheet_id": None
        }
    
    if not spreadsheet_id:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheet_id": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not ranges:
        return {
            "successful": False,
            "message": "Error: Ranges must be specified",
            "spreadsheet_id": None,
            "error": "Missing ranges"
        }
    
    if not isinstance(ranges, list):
        return {
            "successful": False,
            "message": "Error: Ranges must be an array",
            "spreadsheet_id": None,
            "error": "Invalid ranges format"
        }
    
    if len(ranges) == 0:
        return {
            "successful": False,
            "message": "Error: At least one range must be specified",
            "spreadsheet_id": None,
            "error": "Empty ranges array"
        }
    
    # Validate that all ranges are strings
    for i, range_item in enumerate(ranges):
        if not isinstance(range_item, str):
            return {
                "successful": False,
                "message": f"Error: Range at index {i} must be a string",
                "spreadsheet_id": None,
                "error": f"Invalid range type at index {i}"
            }
        if not range_item.strip():
            return {
                "successful": False,
                "message": f"Error: Range at index {i} cannot be empty",
                "spreadsheet_id": None,
                "error": f"Empty range at index {i}"
            }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Validate spreadsheet exists
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheet_id,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing spreadsheet: {str(e)}",
                "spreadsheet_id": spreadsheet_id,
                "error": str(e)
            }
        
        # Get spreadsheet title
        spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', 'Unknown')
        
        # Clear the specified ranges
        try:
            response = service.spreadsheets().values().batchClear(
                spreadsheetId=spreadsheet_id,
                body={
                    'ranges': ranges
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error clearing ranges: {str(e)}",
                "spreadsheet_id": spreadsheet_id,
                "error": str(e)
            }
        
        # Extract response data
        cleared_ranges = response.get('clearedRanges', [])
        spreadsheet_id_response = response.get('spreadsheetId', spreadsheet_id)
        
        # Count total cells cleared
        total_cells_cleared = 0
        range_details = []
        
        for cleared_range in cleared_ranges:
            # Parse the range to estimate cell count
            try:
                # Extract sheet name and range
                if '!' in cleared_range:
                    sheet_part, range_part = cleared_range.split('!', 1)
                    sheet_name = sheet_part.strip("'")
                else:
                    sheet_name = "Sheet1"  # Default sheet
                    range_part = cleared_range
                
                # Parse range to get dimensions
                if ':' in range_part:
                    start_cell, end_cell = range_part.split(':')
                    
                    # Convert A1 notation to row/column indices
                    def parse_cell(cell):
                        import re
                        match = re.match(r'([A-Z]+)(\d+)', cell.upper())
                        if match:
                            col_str, row_str = match.groups()
                            # Convert column letters to number
                            col_num = 0
                            for char in col_str:
                                col_num = col_num * 26 + (ord(char) - ord('A') + 1)
                            return int(row_str), col_num
                        return 1, 1
                    
                    start_row, start_col = parse_cell(start_cell)
                    end_row, end_col = parse_cell(end_cell)
                    
                    # Calculate cell count
                    rows = end_row - start_row + 1
                    cols = end_col - start_col + 1
                    cells_in_range = rows * cols
                else:
                    # Single cell
                    cells_in_range = 1
                
                total_cells_cleared += cells_in_range
                
                range_details.append({
                    "range": cleared_range,
                    "sheet_name": sheet_name,
                    "estimated_cells": cells_in_range
                })
                
            except Exception:
                # If parsing fails, assume at least 1 cell
                total_cells_cleared += 1
                range_details.append({
                    "range": cleared_range,
                    "sheet_name": "Unknown",
                    "estimated_cells": 1
                })
        
        return {
            "successful": True,
            "message": f"Successfully cleared {len(cleared_ranges)} range(s) from spreadsheet '{spreadsheet_title}'",
            "spreadsheet_id": spreadsheet_id_response,
            "spreadsheet_title": spreadsheet_title,
            "total_ranges_cleared": len(cleared_ranges),
            "total_cells_cleared": total_cells_cleared,
            "cleared_ranges": cleared_ranges,
            "range_details": range_details,
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id_response}/edit",
            "clear_details": {
                "input_ranges": ranges,
                "successful_ranges": cleared_ranges,
                "failed_ranges": [r for r in ranges if r not in cleared_ranges] if len(cleared_ranges) < len(ranges) else []
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error batch clearing spreadsheet values: {str(e)}",
            "spreadsheet_id": spreadsheet_id,
            "error": str(e)
        }


@simple_mcp.tool()
async def batch_clear_values_by_data_filter(spreadsheetId: str, dataFilters: list) -> dict:
    """Clear one or more ranges of values from a spreadsheet using data filters.
    
    Clears one or more ranges of values from a spreadsheet using data filters. 
    the caller must specify the spreadsheet id and one or more datafilters. 
    ranges matching any of the specified data filters will be cleared. 
    only values are cleared -- all other properties of the cell (such as formatting, data validation, etc..) are kept.
    
    Args:
        spreadsheetId: The ID of the spreadsheet to clear values from
        dataFilters: Array of data filters to match ranges for clearing
        Example: "dataFilters": ["Sheet1!A1:B5","Sheet1!D3:F8","Sheet2!C1:C10"]
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheetId": None
        }
    
    if not spreadsheetId:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheetId": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not dataFilters:
        return {
            "successful": False,
            "message": "Error: Data filters must be specified",
            "spreadsheetId": None,
            "error": "Missing data filters"
        }
    
    if not isinstance(dataFilters, list):
        return {
            "successful": False,
            "message": "Error: Data filters must be an array",
            "spreadsheetId": None,
            "error": "Invalid data filters format"
        }
    
    if len(dataFilters) == 0:
        return {
            "successful": False,
            "message": "Error: At least one data filter must be specified",
            "spreadsheetId": None,
            "error": "Empty data filters array"
        }
    
    # Convert A1 notation strings to DataFilter objects if needed
    processed_data_filters = []
    for i, filter_item in enumerate(dataFilters):
        if isinstance(filter_item, str):
            # Convert A1 notation string to DataFilter object
            processed_data_filters.append({
                'a1Range': filter_item
            })
        elif isinstance(filter_item, dict):
            # Already a DataFilter object
            processed_data_filters.append(filter_item)
        else:
            return {
                "successful": False,
                "message": f"Error: Data filter at index {i} must be a string or object",
                "spreadsheetId": None,
                "error": f"Invalid data filter type at index {i}"
            }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Validate spreadsheet exists
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing spreadsheet: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Get spreadsheet title
        spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', 'Unknown')
        
        # Clear values using data filters
        try:
            response = service.spreadsheets().values().batchClearByDataFilter(
                spreadsheetId=spreadsheetId,
                body={
                    'dataFilters': processed_data_filters
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error clearing values by data filter: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Extract response data
        cleared_ranges = response.get('clearedRanges', [])
        spreadsheet_id_response = response.get('spreadsheetId', spreadsheetId)
        
        # Count total cells cleared
        total_cells_cleared = 0
        range_details = []
        
        for cleared_range in cleared_ranges:
            # Parse the range to estimate cell count
            try:
                # Extract sheet name and range
                if '!' in cleared_range:
                    sheet_part, range_part = cleared_range.split('!', 1)
                    sheet_name = sheet_part.strip("'")
                else:
                    sheet_name = "Sheet1"  # Default sheet
                    range_part = cleared_range
                
                # Parse range to get dimensions
                if ':' in range_part:
                    start_cell, end_cell = range_part.split(':')
                    
                    # Convert A1 notation to row/column indices
                    def parse_cell(cell):
                        import re
                        match = re.match(r'([A-Z]+)(\d+)', cell.upper())
                        if match:
                            col_str, row_str = match.groups()
                            # Convert column letters to number
                            col_num = 0
                            for char in col_str:
                                col_num = col_num * 26 + (ord(char) - ord('A') + 1)
                            return int(row_str), col_num
                        return 1, 1
                    
                    start_row, start_col = parse_cell(start_cell)
                    end_row, end_col = parse_cell(end_cell)
                    
                    # Calculate cell count
                    rows = end_row - start_row + 1
                    cols = end_col - start_col + 1
                    cells_in_range = rows * cols
                else:
                    # Single cell
                    cells_in_range = 1
                
                total_cells_cleared += cells_in_range
                
                range_details.append({
                    "range": cleared_range,
                    "sheet_name": sheet_name,
                    "estimated_cells": cells_in_range
                })
                
            except Exception:
                # If parsing fails, assume at least 1 cell
                total_cells_cleared += 1
                range_details.append({
                    "range": cleared_range,
                    "sheet_name": "Unknown",
                    "estimated_cells": 1
                })
        
        return {
            "successful": True,
            "message": f"Successfully cleared {len(cleared_ranges)} range(s) from spreadsheet '{spreadsheet_title}' using data filters",
            "spreadsheetId": spreadsheet_id_response,
            "spreadsheet_title": spreadsheet_title,
            "total_ranges_cleared": len(cleared_ranges),
            "total_cells_cleared": total_cells_cleared,
            "cleared_ranges": cleared_ranges,
            "range_details": range_details,
            "data_filters_used": len(processed_data_filters),
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id_response}/edit",
            "clear_details": {
                "input_data_filters": dataFilters,
                "processed_data_filters": processed_data_filters,
                "successful_ranges": cleared_ranges,
                "method": "data_filter_clear"
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error batch clearing values by data filter: {str(e)}",
            "spreadsheetId": spreadsheetId,
            "error": str(e)
        }


@simple_mcp.tool()
async def batch_get_spreadsheet_values_by_data_filter(spreadsheetId: str, dataFilters: list, majorDimension: str = "ROWS", valueRenderOption: str = "UNFORMATTED_VALUE", dateTimeRenderOption: str = "SERIAL_NUMBER") -> dict:
    """Return one or more ranges of values from a spreadsheet that match the specified data filters.
    
    Tool to return one or more ranges of values from a spreadsheet that match the specified data filters. 
    use when you need to retrieve specific data sets based on filtering criteria rather than entire sheets or fixed ranges.
    
    Args:
        spreadsheetId: The ID of the spreadsheet to get values from
        dataFilters: Array of data filters to match ranges for retrieving values
        majorDimension: The major dimension of the values
        valueRenderOption: Determines how values in the response should be rendered
        dateTimeRenderOption: Determines how dates, times, and durations in the response should be rendered
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheetId": None
        }
    
    if not spreadsheetId:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheetId": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not dataFilters:
        return {
            "successful": False,
            "message": "Error: Data filters must be specified",
            "spreadsheetId": None,
            "error": "Missing data filters"
        }
    
    if not isinstance(dataFilters, list):
        return {
            "successful": False,
            "message": "Error: Data filters must be an array",
            "spreadsheetId": None,
            "error": "Invalid data filters format"
        }
    
    if len(dataFilters) == 0:
        return {
            "successful": False,
            "message": "Error: At least one data filter must be specified",
            "spreadsheetId": None,
            "error": "Empty data filters array"
        }
    
    # Validate majorDimension
    valid_major_dimensions = ["ROWS", "COLUMNS"]
    if majorDimension not in valid_major_dimensions:
        return {
            "successful": False,
            "message": f"Error: majorDimension must be one of {valid_major_dimensions}",
            "spreadsheetId": None,
            "error": f"Invalid majorDimension: {majorDimension}"
        }
    
    # Validate valueRenderOption
    valid_value_render_options = ["FORMATTED_VALUE", "UNFORMATTED_VALUE", "FORMULA"]
    if valueRenderOption not in valid_value_render_options:
        return {
            "successful": False,
            "message": f"Error: valueRenderOption must be one of {valid_value_render_options}",
            "spreadsheetId": None,
            "error": f"Invalid valueRenderOption: {valueRenderOption}"
        }
    
    # Validate dateTimeRenderOption
    valid_datetime_render_options = ["SERIAL_NUMBER", "FORMATTED_STRING"]
    if dateTimeRenderOption not in valid_datetime_render_options:
        return {
            "successful": False,
            "message": f"Error: dateTimeRenderOption must be one of {valid_datetime_render_options}",
            "spreadsheetId": None,
            "error": f"Invalid dateTimeRenderOption: {dateTimeRenderOption}"
        }
    
    # Convert A1 notation strings to DataFilter objects if needed
    processed_data_filters = []
    for i, filter_item in enumerate(dataFilters):
        if isinstance(filter_item, str):
            # Convert A1 notation string to DataFilter object
            processed_data_filters.append({
                'a1Range': filter_item
            })
        elif isinstance(filter_item, dict):
            # Already a DataFilter object
            processed_data_filters.append(filter_item)
        else:
            return {
                "successful": False,
                "message": f"Error: Data filter at index {i} must be a string or object",
                "spreadsheetId": None,
                "error": f"Invalid data filter type at index {i}"
            }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Validate spreadsheet exists
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing spreadsheet: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Get spreadsheet title
        spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', 'Unknown')
        
        # Get values using data filters
        try:
            response = service.spreadsheets().values().batchGetByDataFilter(
                spreadsheetId=spreadsheetId,
                body={
                    'dataFilters': processed_data_filters,
                    'majorDimension': majorDimension,
                    'valueRenderOption': valueRenderOption,
                    'dateTimeRenderOption': dateTimeRenderOption
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error getting values by data filter: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Extract response data
        value_ranges = response.get('valueRanges', [])
        spreadsheet_id_response = response.get('spreadsheetId', spreadsheetId)
        
        # Process the retrieved data
        total_ranges_retrieved = len(value_ranges)
        total_cells_retrieved = 0
        range_details = []
        all_values = []
        
        for value_range in value_ranges:
            range_name = value_range.get('range', 'Unknown')
            values = value_range.get('valueRange', {}).get('values', [])
            
            # Count cells in this range
            cells_in_range = sum(len(row) for row in values) if values else 0
            total_cells_retrieved += cells_in_range
            
            # Extract sheet name from range
            sheet_name = "Unknown"
            if '!' in range_name:
                sheet_part = range_name.split('!')[0]
                sheet_name = sheet_part.strip("'")
            
            range_details.append({
                "range": range_name,
                "sheet_name": sheet_name,
                "rows": len(values),
                "columns": len(values[0]) if values and len(values) > 0 else 0,
                "cells": cells_in_range,
                "values": values
            })
            
            # Add to all values
            all_values.extend(values)
        
        return {
            "successful": True,
            "message": f"Successfully retrieved {total_ranges_retrieved} range(s) from spreadsheet '{spreadsheet_title}' using data filters",
            "spreadsheetId": spreadsheet_id_response,
            "spreadsheet_title": spreadsheet_title,
            "total_ranges_retrieved": total_ranges_retrieved,
            "total_cells_retrieved": total_cells_retrieved,
            "majorDimension": majorDimension,
            "valueRenderOption": valueRenderOption,
            "dateTimeRenderOption": dateTimeRenderOption,
            "range_details": range_details,
            "all_values": all_values,
            "data_filters_used": len(processed_data_filters),
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id_response}/edit",
            "retrieval_details": {
                "input_data_filters": dataFilters,
                "processed_data_filters": processed_data_filters,
                "successful_ranges": [r["range"] for r in range_details],
                "method": "data_filter_get",
                "total_rows": sum(r["rows"] for r in range_details),
                "total_columns": max(r["columns"] for r in range_details) if range_details else 0
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error batch getting spreadsheet values by data filter: {str(e)}",
            "spreadsheetId": spreadsheetId,
            "error": str(e)
        }


@simple_mcp.tool()
async def update_sheet_properties(spreadsheetId: str, updateSheetProperties: dict) -> dict:
    """Update properties of a sheet (worksheet) within a google spreadsheet.
    
    Tool to update properties of a sheet (worksheet) within a google spreadsheet, 
    such as its title, index, visibility, tab color, or grid properties. 
    use this when you need to modify the metadata or appearance of a specific sheet.
    
    Args:
        spreadsheetId: The ID of the spreadsheet containing the sheet to update
        updateSheetProperties: Object containing the sheet properties to update
        
    Example inputs:
    
    Update sheet title:
    {
        "spreadsheetId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        "updateSheetProperties": {
            "properties": {
                "sheetId": 1234567890,
                "title": "New Sheet Name"
            },
            "fields": "title"
        }
    }
    
    Update sheet visibility and tab color:
    {
        "spreadsheetId": "1cpbQqijz-AS5bvZ32sNTJXKCkhX8GSD9IwuWpeHH_kE",
        "updateSheetProperties": {
            "properties": {
                "sheetId": 9876543210,
                "hidden": false,
                "tabColorStyle": {
                    "rgbColor": {
                        "red": 0.8,
                        "green": 0.2,
                        "blue": 0.2,
                        "alpha": 1.0
                    }
                }
            },
            "fields": "hidden,tabColorStyle"
        }
    }
    
    Update grid properties:
    {
        "spreadsheetId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        "updateSheetProperties": {
            "properties": {
                "sheetId": 1234567890,
                "gridProperties": {
                    "frozenRowCount": 1,
                    "frozenColumnCount": 1,
                    "hideGridlines": false
                }
            },
            "fields": "gridProperties"
        }
    }
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheetId": None
        }
    
    if not spreadsheetId:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheetId": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not updateSheetProperties:
        return {
            "successful": False,
            "message": "Error: Update sheet properties must be specified",
            "spreadsheetId": None,
            "error": "Missing update sheet properties"
        }
    
    if not isinstance(updateSheetProperties, dict):
        return {
            "successful": False,
            "message": "Error: Update sheet properties must be an object",
            "spreadsheetId": None,
            "error": "Invalid update sheet properties format"
        }
    
    # Validate required fields in updateSheetProperties
    if 'properties' not in updateSheetProperties:
        return {
            "successful": False,
            "message": "Error: 'properties' field is required in updateSheetProperties",
            "spreadsheetId": None,
            "error": "Missing properties field"
        }
    
    if 'fields' not in updateSheetProperties:
        return {
            "successful": False,
            "message": "Error: 'fields' field is required in updateSheetProperties",
            "spreadsheetId": None,
            "error": "Missing fields field"
        }
    
    properties = updateSheetProperties.get('properties', {})
    fields = updateSheetProperties.get('fields', '')
    
    if not properties:
        return {
            "successful": False,
            "message": "Error: Properties object cannot be empty",
            "spreadsheetId": None,
            "error": "Empty properties object"
        }
    
    if not fields:
        return {
            "successful": False,
            "message": "Error: Fields string cannot be empty",
            "spreadsheetId": None,
            "error": "Empty fields string"
        }
    
    # Validate that sheetId is present in properties
    if 'sheetId' not in properties:
        return {
            "successful": False,
            "message": "Error: 'sheetId' is required in properties",
            "spreadsheetId": None,
            "error": "Missing sheetId in properties"
        }
    
    sheet_id = properties.get('sheetId')
    if not isinstance(sheet_id, int):
        return {
            "successful": False,
            "message": "Error: sheetId must be an integer",
            "spreadsheetId": None,
            "error": "Invalid sheetId type"
        }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Validate spreadsheet exists and get current sheet info
        try:
            spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing spreadsheet: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Find the sheet to update
        target_sheet = None
        original_properties = {}
        for sheet in spreadsheet_info.get('sheets', []):
            if sheet['properties']['sheetId'] == sheet_id:
                target_sheet = sheet
                original_properties = sheet['properties'].copy()
                break
        
        if not target_sheet:
            available_sheets = [f"{sheet['properties']['title']} (ID: {sheet['properties']['sheetId']})" 
                              for sheet in spreadsheet_info.get('sheets', [])]
            return {
                "successful": False,
                "message": f"Sheet with ID {sheet_id} not found in spreadsheet",
                "spreadsheetId": spreadsheetId,
                "available_sheets": available_sheets,
                "error": f"Sheet ID {sheet_id} not found"
            }
        
        # Get spreadsheet title
        spreadsheet_title = spreadsheet_info.get('properties', {}).get('title', 'Unknown')
        original_sheet_title = original_properties.get('title', 'Unknown')
        
        # Update the sheet properties
        try:
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheetId,
                body={
                    'requests': [
                        {
                            'updateSheetProperties': updateSheetProperties
                        }
                    ]
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error updating sheet properties: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Get updated spreadsheet info to see the changes
        try:
            updated_spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
            
            # Find the updated sheet
            updated_sheet = None
            for sheet in updated_spreadsheet_info.get('sheets', []):
                if sheet['properties']['sheetId'] == sheet_id:
                    updated_sheet = sheet
                    break
            
            updated_properties = updated_sheet['properties'] if updated_sheet else {}
            
        except Exception:
            # If we can't get updated info, use the original properties
            updated_properties = original_properties
        
        # Determine what was updated
        updated_fields = fields.split(',')
        changes_made = []
        
        for field in updated_fields:
            field = field.strip()
            if field == 'title' and 'title' in properties:
                new_title = properties.get('title')
                if new_title != original_properties.get('title'):
                    changes_made.append(f"Title: '{original_properties.get('title', 'Unknown')}' → '{new_title}'")
            
            elif field == 'hidden' and 'hidden' in properties:
                new_hidden = properties.get('hidden')
                if new_hidden != original_properties.get('hidden'):
                    changes_made.append(f"Hidden: {original_properties.get('hidden', False)} → {new_hidden}")
            
            elif field == 'index' and 'index' in properties:
                new_index = properties.get('index')
                if new_index != original_properties.get('index'):
                    changes_made.append(f"Index: {original_properties.get('index', 0)} → {new_index}")
            
            elif field == 'tabColorStyle' and 'tabColorStyle' in properties:
                changes_made.append("Tab color updated")
            
            elif field == 'gridProperties' and 'gridProperties' in properties:
                changes_made.append("Grid properties updated")
        
        return {
            "successful": True,
            "message": f"Successfully updated sheet properties in spreadsheet '{spreadsheet_title}'",
            "spreadsheetId": spreadsheetId,
            "spreadsheet_title": spreadsheet_title,
            "sheet_id": sheet_id,
            "original_sheet_title": original_sheet_title,
            "updated_sheet_title": updated_properties.get('title', original_sheet_title),
            "changes_made": changes_made,
            "fields_updated": updated_fields,
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheetId}/edit",
            "update_details": {
                "original_properties": original_properties,
                "updated_properties": updated_properties,
                "requested_properties": properties,
                "fields_requested": fields
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error updating sheet properties: {str(e)}",
            "spreadsheetId": spreadsheetId,
            "error": str(e)
        }


@simple_mcp.tool()
async def update_spreadsheet_properties(spreadsheetId: str, properties: dict, fields: str) -> dict:
    """Update properties of a spreadsheet, such as its title, locale, or auto-recalculation settings.
    
    Tool to update properties of a spreadsheet, such as its title, locale, or auto-recalculation settings. 
    use when you need to modify the overall configuration of a google sheet.
    
    Args:
        spreadsheetId: The ID of the spreadsheet to update
        properties: Object containing the spreadsheet properties to update
        fields: Comma-separated string specifying which properties to update
        
    Example inputs:
    
    Update spreadsheet title:
    {
        "spreadsheetId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        "properties": {
            "title": "New Spreadsheet Title"
        },
        "fields": "title"
    }
    
    Update title and locale:
    {
        "spreadsheetId": "1cpbQqijz-AS5bvZ32sNTJXKCkhX8GSD9IwuWpeHH_kE",
        "properties": {
            "title": "Updated Project Data",
            "locale": "en_US"
        },
        "fields": "title,locale"
    }
    
    Update auto-recalculation settings:
    {
        "spreadsheetId": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        "properties": {
            "autoRecalc": "ON_CHANGE"
        },
        "fields": "autoRecalc"
    }
    
    Update time zone and locale:
    {
        "spreadsheetId": "1cpbQqijz-AS5bvZ32sNTJXKCkhX8GSD9IwuWpeHH_kE",
        "properties": {
            "timeZone": "America/New_York",
            "locale": "en_US"
        },
        "fields": "timeZone,locale"
    }
    """
    # Check credentials first
    creds_valid, error_msg = check_credentials()
    if not creds_valid:
        error_response = get_auth_error_response()
        return {
            "successful": False,
            "message": error_response["message"],
            "error": error_response["error"],
            "instructions": error_response["instructions"],
            "spreadsheetId": None
        }
    
    if not spreadsheetId:
        return {
            "successful": False,
            "message": "Error: Spreadsheet ID must be specified",
            "spreadsheetId": None,
            "error": "Missing spreadsheet ID"
        }
    
    if not properties:
        return {
            "successful": False,
            "message": "Error: Properties must be specified",
            "spreadsheetId": None,
            "error": "Missing properties"
        }
    
    if not isinstance(properties, dict):
        return {
            "successful": False,
            "message": "Error: Properties must be an object",
            "spreadsheetId": None,
            "error": "Invalid properties format"
        }
    
    if not fields:
        return {
            "successful": False,
            "message": "Error: Fields must be specified",
            "spreadsheetId": None,
            "error": "Missing fields"
        }
    
    if not isinstance(fields, str):
        return {
            "successful": False,
            "message": "Error: Fields must be a string",
            "spreadsheetId": None,
            "error": "Invalid fields format"
        }
    
    # Validate fields string
    if not fields.strip():
        return {
            "successful": False,
            "message": "Error: Fields string cannot be empty",
            "spreadsheetId": None,
            "error": "Empty fields string"
        }
    
    # Validate autoRecalc value if present
    if 'autoRecalc' in properties:
        valid_auto_recalc_values = ["RECALCULATION_NEVER", "ON_CHANGE", "MINUTE"]
        auto_recalc_value = properties.get('autoRecalc')
        if auto_recalc_value not in valid_auto_recalc_values:
            return {
                "successful": False,
                "message": f"Error: autoRecalc must be one of {valid_auto_recalc_values}",
                "spreadsheetId": None,
                "error": f"Invalid autoRecalc value: {auto_recalc_value}"
            }
    
    try:
        # Build the Sheets service
        service = build('sheets', 'v4', credentials=creds)
        
        # Get current spreadsheet info to compare with
        try:
            original_spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error accessing spreadsheet: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Get original properties
        original_properties = original_spreadsheet_info.get('properties', {})
        original_title = original_properties.get('title', 'Unknown')
        
        # Update the spreadsheet properties
        try:
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheetId,
                body={
                    'requests': [
                        {
                            'updateSpreadsheetProperties': {
                                'properties': properties,
                                'fields': fields
                            }
                        }
                    ]
                }
            ).execute()
        except Exception as e:
            return {
                "successful": False,
                "message": f"Error updating spreadsheet properties: {str(e)}",
                "spreadsheetId": spreadsheetId,
                "error": str(e)
            }
        
        # Get updated spreadsheet info to see the changes
        try:
            updated_spreadsheet_info = service.spreadsheets().get(
                spreadsheetId=spreadsheetId,
                ranges=[],
                includeGridData=False
            ).execute()
            
            updated_properties = updated_spreadsheet_info.get('properties', {})
            
        except Exception:
            # If we can't get updated info, use the original properties
            updated_properties = original_properties
        
        # Determine what was updated
        updated_fields = [field.strip() for field in fields.split(',')]
        changes_made = []
        
        for field in updated_fields:
            if field == 'title' and 'title' in properties:
                new_title = properties.get('title')
                if new_title != original_properties.get('title'):
                    changes_made.append(f"Title: '{original_properties.get('title', 'Unknown')}' → '{new_title}'")
            
            elif field == 'locale' and 'locale' in properties:
                new_locale = properties.get('locale')
                if new_locale != original_properties.get('locale'):
                    changes_made.append(f"Locale: {original_properties.get('locale', 'Unknown')} → {new_locale}")
            
            elif field == 'timeZone' and 'timeZone' in properties:
                new_timezone = properties.get('timeZone')
                if new_timezone != original_properties.get('timeZone'):
                    changes_made.append(f"Time Zone: {original_properties.get('timeZone', 'Unknown')} → {new_timezone}")
            
            elif field == 'autoRecalc' and 'autoRecalc' in properties:
                new_auto_recalc = properties.get('autoRecalc')
                if new_auto_recalc != original_properties.get('autoRecalc'):
                    changes_made.append(f"Auto Recalc: {original_properties.get('autoRecalc', 'Unknown')} → {new_auto_recalc}")
        
        return {
            "successful": True,
            "message": f"Successfully updated spreadsheet properties",
            "spreadsheetId": spreadsheetId,
            "original_title": original_title,
            "updated_title": updated_properties.get('title', original_title),
            "changes_made": changes_made,
            "fields_updated": updated_fields,
            "web_view_link": f"https://docs.google.com/spreadsheets/d/{spreadsheetId}/edit",
            "update_details": {
                "original_properties": original_properties,
                "updated_properties": updated_properties,
                "requested_properties": properties,
                "fields_requested": fields
            }
        }
        
    except Exception as e:
        return {
            "successful": False,
            "message": f"Error updating spreadsheet properties: {str(e)}",
            "spreadsheetId": spreadsheetId,
            "error": str(e)
        }


@simple_mcp.tool()
async def demonstrate_pagination() -> dict:
    """Demonstrate how to use pagination with the updated tools
    
    This tool shows you how to use the new pagination features in:
    - list_sheets: Use page_token for Google Drive API pagination
    - search_spreadsheets: Use page_token for Google Drive API pagination  
    - get_sheet_names: Use start_index and max_sheets for manual pagination
    - list_tables: Use start_index and max_tables for manual pagination
    """
    
    return {
        "successful": True,
        "message": "Pagination Guide for Google Sheets MCP Tools",
        "pagination_examples": {
            "list_sheets": {
                "description": "List Google Sheets with Google Drive API pagination",
                "parameters": {
                    "max_results": "Maximum sheets to return (1-1000, default: 50)",
                    "page_token": "Token for next page (from previous response)"
                },
                "usage": [
                    "1. Call list_sheets(max_results=10) to get first 10 sheets",
                    "2. Check response.pagination.has_more for more results",
                    "3. Use response.pagination.next_page_token for next page",
                    "4. Call list_sheets(max_results=10, page_token=token) for next page"
                ],
                "response_structure": {
                    "pagination": {
                        "has_more": "Boolean indicating if more results exist",
                        "next_page_token": "Token for next page (or null if no more)",
                        "total_estimated": "Rough estimate of total results"
                    },
                    "summary": {
                        "returned_count": "Number of results in this response",
                        "max_results": "Maximum results requested"
                    }
                }
            },
            "search_spreadsheets": {
                "description": "Search spreadsheets with Google Drive API pagination",
                "parameters": {
                    "max_results": "Maximum results to return (1-1000, default: 10)",
                    "page_token": "Token for next page (from previous response)"
                },
                "usage": [
                    "1. Call search_spreadsheets(query='name contains test', max_results=5)",
                    "2. Check response.pagination.has_more for more results",
                    "3. Use response.pagination.next_page_token for next page",
                    "4. Call search_spreadsheets(..., page_token=token) for next page"
                ],
                "response_structure": {
                    "pagination": {
                        "has_more": "Boolean indicating if more results exist",
                        "next_page_token": "Token for next page (or null if no more)",
                        "total_estimated": "Rough estimate of total results"
                    }
                }
            },
            "get_sheet_names": {
                "description": "Get worksheet names with manual pagination",
                "parameters": {
                    "max_sheets": "Maximum sheets to return (1-1000, default: 100)",
                    "start_index": "Starting index for pagination (0-based, default: 0)"
                },
                "usage": [
                    "1. Call get_sheet_names(spreadsheet_id, max_sheets=10) for first 10",
                    "2. Check response.pagination.has_more for more results",
                    "3. Use response.pagination.next_start_index for next page",
                    "4. Call get_sheet_names(..., start_index=10) for next page"
                ],
                "response_structure": {
                    "pagination": {
                        "has_more": "Boolean indicating if more results exist",
                        "next_start_index": "Starting index for next page (or null if no more)",
                        "total_sheets": "Total number of sheets in spreadsheet",
                        "returned_count": "Number of results in this response",
                        "start_index": "Starting index used for this request",
                        "end_index": "Ending index of results (0-based)"
                    }
                }
            },
            "list_tables": {
                "description": "List tables with manual pagination",
                "parameters": {
                    "max_tables": "Maximum tables to return (1-1000, default: 50)",
                    "start_index": "Starting index for pagination (0-based, default: 0)"
                },
                "usage": [
                    "1. Call list_tables(spreadsheet_id, max_tables=10) for first 10 tables",
                    "2. Check response.pagination.has_more for more results",
                    "3. Use response.pagination.next_start_index for next page",
                    "4. Call list_tables(..., start_index=10) for next page"
                ],
                "response_structure": {
                    "pagination": {
                        "has_more": "Boolean indicating if more results exist",
                        "next_start_index": "Starting index for next page (or null if no more)",
                        "total_tables": "Total number of tables found",
                        "returned_count": "Number of results in this response",
                        "start_index": "Starting index used for this request",
                        "end_index": "Ending index of results (0-based)"
                    }
                }
            }
        },
        "best_practices": [
            "Use smaller max_results values (10-50) for better performance",
            "Always check pagination.has_more before requesting next page",
            "For Google Drive API tools (list_sheets, search_spreadsheets): use page_token",
            "For manual pagination tools (get_sheet_names, list_tables): use start_index",
            "Store pagination tokens/indexes if you need to resume later",
            "Consider caching results for frequently accessed data"
        ],
        "example_workflow": [
            "1. Start with small max_results (e.g., 10-20)",
            "2. Process the current page of results",
            "3. Check if more results exist (pagination.has_more)",
            "4. If yes, use the next page token/index to get more results",
            "5. Repeat until all results are processed"
        ]
    }


if __name__ == "__main__":
    simple_mcp.run()
    # simple_mcp.run(transport="http", host="127.0.0.1", port=8000)

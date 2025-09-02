#!/usr/bin/env python3
"""
Google Sheets Authentication Script

This script helps you authenticate with Google Sheets API and save your credentials.
Run this script once to set up your authentication tokens.

Usage:
    uv run authenticate.py
"""

import json
import os
import webbrowser
from pathlib import Path
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Scopes for Google Sheets and Drive access
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

def main():
    print("🔐 Google Sheets Authentication Setup")
    print("=" * 50)
    
    # Get paths from environment variables or use defaults
    credentials_path = os.getenv('GSHEETS_CREDENTIALS_PATH', './credentials.json')
    token_path = os.getenv('GSHEETS_TOKEN_PATH', './.token.json')
    
    credentials_file = Path(credentials_path)
    token_file = Path(token_path)
    
    print(f"📁 Credentials file: {credentials_path}")
    print(f"💾 Token file: {token_path}")
    print()
    
    # Check if credentials file exists
    if not credentials_file.exists():
        print(f"❌ Error: Credentials file not found at {credentials_path}")
        print()
        print("📋 To fix this:")
        print("1. Go to Google Cloud Console (https://console.cloud.google.com/)")
        print("2. Create a project or select existing one")
        print("3. Enable Google Sheets API and Google Drive API")
        print("4. Create OAuth 2.0 credentials")
        print("5. Download the credentials.json file")
        print("6. Place it in this project directory")
        print("7. Run this script again")
        print()
        print("💡 Or set the GSHEETS_CREDENTIALS_PATH environment variable:")
        print(f"   export GSHEETS_CREDENTIALS_PATH='/path/to/your/credentials.json'")
        return
    
    # Load client configuration
    try:
        with open(credentials_file, 'r') as f:
            client_config = json.load(f)
        
        # Handle both web and installed app types
        if 'installed' in client_config:
            client_data = client_config['installed']
        elif 'web' in client_config:
            client_data = client_config['web']
        else:
            print("❌ Invalid credentials file format")
            print("Expected 'installed' or 'web' section in credentials.json")
            return
        
        client_id = client_data['client_id']
        client_secret = client_data['client_secret']
        redirect_uri = client_data['redirect_uris'][0]
        
        print("✅ Credentials file loaded successfully")
        
    except Exception as e:
        print(f"❌ Error loading credentials: {e}")
        print("Please check your credentials.json file format")
        return
    
    # Check if token already exists and is valid
    if token_file.exists():
        try:
            token_data = json.loads(token_file.read_text())
            creds = Credentials.from_authorized_user_info(token_data, SCOPES)
            
            if creds and creds.valid:
                print("✅ Valid token found! You're already authenticated.")
                print("You can now use your Google Sheets tools.")
                return
            elif creds and creds.expired and creds.refresh_token:
                print("🔄 Token expired, refreshing...")
                try:
                    creds.refresh(Request())
                    token_file.write_text(creds.to_json())
                    print("✅ Token refreshed successfully!")
                    return
                except Exception as refresh_error:
                    print(f"⚠️  Token refresh failed: {refresh_error}")
                    print("Starting new authentication...")
            else:
                print("⚠️  Token found but invalid, starting new authentication...")
        except Exception as e:
            print(f"⚠️  Error with existing token: {e}")
            print("Starting new authentication...")
    
    # Start OAuth flow
    print("🚀 Starting OAuth authentication...")
    
    try:
        flow = Flow.from_client_config(
            {
                "installed": {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "redirect_uris": [redirect_uri],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            },
            scopes=SCOPES,
            redirect_uri=redirect_uri,
        )
        
        # Generate authorization URL
        auth_url, _ = flow.authorization_url(
            prompt="consent", 
            access_type="offline",
            include_granted_scopes="true"
        )
        
        print(f"🌐 Authorization URL: {auth_url}")
        print()
        
        # Try to open browser automatically
        try:
            webbrowser.open(auth_url)
            print("✅ Browser opened automatically!")
        except Exception:
            print("⚠️  Could not open browser automatically.")
        
        print("📋 Please:")
        print("1. Complete the authorization in your browser")
        print("2. Copy the authorization code from the page")
        print("3. Paste it below")
        print()
        
        # Get authorization code from user
        auth_code = input("Enter authorization code: ").strip()
        
        if not auth_code:
            print("❌ No authorization code provided")
            return
        
        # Exchange authorization code for tokens
        flow.fetch_token(code=auth_code)
        creds = flow.credentials
        
        # Save credentials
        token_file.write_text(creds.to_json())
        
        print("✅ Authentication successful!")
        print(f"💾 Token saved to: {token_path}")
        print()
        print("🎉 You can now use your Google Sheets tools!")
        print()
        print("🚀 To start your MCP server:")
        print("   uv run simplemcp.py")
        print()
        print("🔧 To test with MCP Inspector:")
        print("   npx @modelcontextprotocol/inspector")
        print()
        print("📝 Environment variables (optional):")
        print(f"   export GSHEETS_CREDENTIALS_PATH='{credentials_path}'")
        print(f"   export GSHEETS_TOKEN_PATH='{token_path}'")
        
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        print("Please try again with a valid authorization code.")
        print()
        print("💡 Common issues:")
        print("- Authorization code expired (get a fresh one)")
        print("- Invalid redirect URI in Google Cloud Console")
        print("- Network connectivity issues")

if __name__ == "__main__":
    main()

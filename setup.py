#!/usr/bin/env python3
"""
Quick Setup Script for Google Sheets MCP Server

This script helps you get started quickly by checking prerequisites
and guiding you through the setup process.
"""

import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 12):
        print("❌ Python 3.12+ is required")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python {sys.version_info.major}.{sys.version_info.minor} detected")
    return True

def check_uv():
    """Check if UV is installed"""
    try:
        result = subprocess.run(['uv', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ UV detected: {result.stdout.strip()}")
            return True
        else:
            print("❌ UV not found")
            return False
    except FileNotFoundError:
        print("❌ UV not found")
        return False

def check_credentials():
    """Check if credentials file exists"""
    creds_file = Path('credentials.json')
    if creds_file.exists():
        print("✅ credentials.json found")
        return True
    else:
        print("❌ credentials.json not found")
        return False

def check_token():
    """Check if token file exists"""
    token_file = Path('.token.json')
    if token_file.exists():
        print("✅ .token.json found")
        return True
    else:
        print("❌ .token.json not found")
        return False

def install_dependencies():
    """Install project dependencies"""
    print("\n📦 Installing dependencies...")
    try:
        result = subprocess.run(['uv', 'sync'], check=True, capture_output=True, text=True)
        print("✅ Dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        print(f"   Error output: {e.stderr}")
        return False

def main():
    print("🚀 Google Sheets MCP Server Setup")
    print("=" * 40)
    
    # Check prerequisites
    print("\n🔍 Checking prerequisites...")
    python_ok = check_python_version()
    uv_ok = check_uv()
    
    if not python_ok or not uv_ok:
        print("\n❌ Prerequisites not met. Please:")
        if not python_ok:
            print("   - Install Python 3.12+")
        if not uv_ok:
            print("   - Install UV: curl -LsSf https://astral.sh/uv/install.sh | sh")
        return
    
    # Install dependencies
    if not install_dependencies():
        return
    
    # Check authentication status
    print("\n🔐 Checking authentication...")
    creds_ok = check_credentials()
    token_ok = check_token()
    
    if not creds_ok:
        print("\n📋 To get credentials:")
        print("1. Go to Google Cloud Console (https://console.cloud.google.com/)")
        print("2. Create a project or select existing one")
        print("3. Enable Google Sheets API and Google Drive API")
        print("4. Create OAuth 2.0 credentials")
        print("5. Download credentials.json and place it in this directory")
    
    if not token_ok:
        print("\n🔑 To authenticate:")
        print("   uv run authenticate.py")
    
    if creds_ok and token_ok:
        print("\n🎉 Setup complete! You can now:")
        print("   Start server: uv run simplemcp.py")
        print("   Test tools: npx @modelcontextprotocol/inspector")
    
    print("\n📚 For more information, see README.md")

if __name__ == "__main__":
    main()

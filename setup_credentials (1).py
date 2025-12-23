"""
setup_credentials.py
===========================================================
HELPER: Prepare Google Credentials for Deployment
===========================================================

Problem:
  Deployment platforms (Render, Heroku) and .env files require 
  the Google Service Account JSON to be a SINGLE LINE string.
  Copy-pasting the JSON file directly often breaks the 
  "private_key" field due to newlines.

Solution:
  This script reads 'credentials.json', minifies it, 
  fixes newline escaping, and outputs the exact string to use.

Usage:
  1. Place your Google 'credentials.json' in this folder.
  2. Run: python setup_credentials.py
  3. Copy the output line.
"""

import json
import os
import sys

INPUT_FILENAME = "credentials.json"

def main():
    print(f"--- Google Credentials Formatter ---\n")
    
    # 1. Check file existence
    if not os.path.exists(INPUT_FILENAME):
        print(f"❌ Error: '{INPUT_FILENAME}' not found.")
        print(f"   Please download your Service Account Key from Google Cloud Console,")
        print(f"   rename it to '{INPUT_FILENAME}', and place it in this folder.")
        sys.exit(1)

    # 2. Read and Parse
    try:
        with open(INPUT_FILENAME, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError:
        print(f"❌ Error: '{INPUT_FILENAME}' is not valid JSON.")
        sys.exit(1)

    # 3. Validate critical fields
    required = ["type", "project_id", "private_key", "client_email"]
    missing = [k for k in required if k not in data]
    if missing:
        print(f"❌ Error: JSON is missing standard fields: {missing}")
        sys.exit(1)

    # 4. Flatten and Escape
    # We dump it to a string. json.dumps automatically escapes newlines in the private key
    # into \n, and puts everything on one line if indent is None.
    flat_json = json.dumps(data, separators=(',', ':'))

    # 5. Output
    print("✅ Success! Your JSON is formatted.")
    print("\nCopy the line below (everything between the arrows):")
    print("-" * 60)
    print("▼ ▼ ▼")
    print(flat_json)
    print("▲ ▲ ▲")
    print("-" * 60)
    
    print("\nInstructions:")
    print("1. Local: Paste into 'env.local' as:")
    print("   GOOGLE_SHEETS_CREDENTIALS=" + flat_json[:20] + "...")
    print("2. Render: Add Environment Variable 'GOOGLE_SHEETS_CREDENTIALS'")
    print("   Value: Paste the string above.")

if __name__ == "__main__":
    main()

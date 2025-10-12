import os, time, json, math
from typing import List, Dict, Any
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx

APP_TOKEN = os.getenv("APP_TOKEN", "change-me")
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36")
# ... (keep all your code exactly as you pasted, down to the final return in /v33/fund)

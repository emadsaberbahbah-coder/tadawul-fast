# google_apps_script_client.py
import os
import requests
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class GoogleAppsScriptResponse:
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    execution_time: Optional[float] = None

class GoogleAppsScriptClient:
    def __init__(self):
        self.base_url = os.getenv(
            'GOOGLE_APPS_SCRIPT_URL',
            'https://script.google.com/macros/s/AKfycbwnIX0hIaffDJVnHZUxej4zoLPQZgpdMMpkA9YP1xPQVxqwvEAXuIHWcF7qBIVsntnLkg/exec'
        )
        self.timeout = 30

    def get_symbols_data(self, symbol: str = None) -> GoogleAppsScriptResponse:
        """Get symbols data from Google Apps Script."""
        try:
            params = {}
            if symbol:
                params['symbol'] = symbol.upper()

            response = requests.get(
                self.base_url,
                params=params,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            data = response.json()
            return GoogleAppsScriptResponse(
                success=True,
                data=data,
                execution_time=response.elapsed.total_seconds()
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Google Apps Script request failed: {e}")
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Request failed: {e}"
            )
        except ValueError as e:
            logger.error(f"Google Apps Script JSON parse failed: {e}")
            return GoogleAppsScriptResponse(
                success=False,
                error=f"JSON parse failed: {e}"
            )

    def update_symbol_data(self, symbol: str, data: Dict[str, Any]) -> GoogleAppsScriptResponse:
        """Update symbol data via Google Apps Script (POST)."""
        try:
            payload = {
                'action': 'update',
                'symbol': symbol.upper(),
                'data': data
            }

            response = requests.post(
                self.base_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return GoogleAppsScriptResponse(
                success=result.get('success', False),
                data=result,
                execution_time=response.elapsed.total_seconds()
            )
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Google Apps Script update failed: {e}")
            return GoogleAppsScriptResponse(
                success=False,
                error=f"Update failed: {e}"
            )

# Global instance
google_apps_script_client = GoogleAppsScriptClient()

"""
Main entry point for the NLP to SQL application
"""

import uvicorn
from config.settings import API_CONFIG, DEBUG

if __name__ == "__main__":
    uvicorn.run(
        "src.api.main:app",
        host=API_CONFIG["host"],
        port=API_CONFIG["port"],
        reload=DEBUG,
        log_level="info" if not DEBUG else "debug"
    ) 
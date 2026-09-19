"""Regenerate the checked-in schema with the service extra installed."""
import json
from pathlib import Path

from magsync.companion.api import create_app

path = Path(__file__).resolve().parents[1] / 'docs/companion-openapi-v1.json'
path.write_text(json.dumps(create_app().openapi(), indent=2, sort_keys=True) + '\n')
print(path)

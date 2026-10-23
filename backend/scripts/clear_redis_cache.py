"""Clear the chatbot QA cache from Redis.

Usage (from the backend folder, with venv active):
    python scripts/clear_redis_cache.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from chatbot.redis_qa_cache import clear_qa_cache  # noqa: E402

deleted = clear_qa_cache()
print(f"Done. Deleted {deleted} cached answer(s) from Redis.")

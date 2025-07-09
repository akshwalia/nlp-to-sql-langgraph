import json
import hashlib
from typing import Dict


class CacheManager:
    """Manages caching functionality for the SQL generator"""
    
    def __init__(self, use_cache: bool = True, cache_file: str = "query_cache.json"):
        self.use_cache = use_cache
        self.cache_file = cache_file
        self.cache = self._load_cache() if use_cache else {}
    
    def _load_cache(self) -> Dict:
        """Load cache from file"""
        try:
            with open(self.cache_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _save_cache(self) -> None:
        """Save cache to file"""
        if not self.use_cache:
            return
        
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            print(f"Error saving cache: {e}")
    
    def _get_question_hash(self, question: str) -> str:
        """Generate hash for question to use as cache key"""
        return hashlib.md5(question.lower().strip().encode()).hexdigest()
    
    def get_cached_result(self, question: str) -> Dict:
        """Get cached result for a question"""
        if not self.use_cache:
            return None
        
        question_hash = self._get_question_hash(question)
        return self.cache.get(question_hash)
    
    def cache_result(self, question: str, result: Dict) -> None:
        """Cache a result for a question"""
        if not self.use_cache:
            return
        
        question_hash = self._get_question_hash(question)
        self.cache[question_hash] = result
        self._save_cache()
    
    def clear_cache(self) -> None:
        """Clear the cache"""
        self.cache = {}
        self._save_cache()
    
    def get_cache_size(self) -> int:
        """Get the number of cached items"""
        return len(self.cache)
    
    def remove_cached_item(self, question: str) -> bool:
        """Remove a specific cached item"""
        if not self.use_cache:
            return False
        
        question_hash = self._get_question_hash(question)
        if question_hash in self.cache:
            del self.cache[question_hash]
            self._save_cache()
            return True
        return False 
# import httpx
# from typing import Optional, Dict, Any
# from configs.config_manager import cfg
# from fastapi import HTTPException
# from models.schemas import SiteProfile

# class CacheClient:
#     def __init__(self):
#         # 1. Trust the config manager to have resolved the ENV var
#         self.base_url = str(cfg.services['cache'].url).strip("\"' ")

#         # 2. Safety check for protocol
#         if not self.base_url.startswith(('http://', 'https://')):
#             self.base_url = f"http://{self.base_url}"
        
#         self.timeout = cfg.services['cache'].timeout
#         print(f"🚀 CacheClient Initialized at: {self.base_url}")
        
#         # 2. Setup persistent client with the same limits as your other layers
#         self.client = httpx.AsyncClient(
#             base_url=self.base_url.rstrip('/'),
#             timeout=5.0,  # Slightly higher than 2.0 to avoid race conditions
#             limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
#         )
#         print(f"✅ CacheClient Initialized with URL: {self.base_url}")

#     async def get(self, domain: str) -> Optional[SiteProfile]: # 👈 Updated Type Hint
#         endpoint = "/cache/site-profile"
#         try:
#             response = await self.client.get(endpoint, params={"domain": domain})
            
#             if response.status_code == 404:
#                 return None
                
#             response.raise_for_status()
#             data = response.json()
            
#             # 1. Extract the dict
#             profile_dict = data.get("site_profile")
            
#             # 2. Only attempt to instantiate if we actually have data
#             if data.get("found") and profile_dict:
#                 return SiteProfile(**profile_dict) # 🚀 Returns a model, logic is happy
                
#             return None
            
#         except Exception as e:
#             print(f"Gateway connection/validation error (Cache GET): {str(e)}")
#             return None
        
        
#     async def set(self, domain: str, site_profile: Any) -> bool:
#         try:
#             # 1. Safely convert to dict only if it's a Pydantic model
#             # Use model_dump for Pydantic V2, dict() for V1
#             if hasattr(site_profile, "model_dump") and callable(site_profile.model_dump):
#                 data = site_profile.model_dump()
#             elif hasattr(site_profile, "dict") and callable(site_profile.dict):
#                 data = site_profile.dict()
#             else:
#                 # It's already a dict or a basic type
#                 data = site_profile

#             payload = {
#                 "domain": domain, 
#                 "site_profile": data
#             }
            
#             # 2. Make the request to the /cache prefix
#             res = await self.client.post("/cache/site-profile", json=payload)
#             return res.status_code == 200
            
#         except Exception as e:
#             print(f"[CacheClient SET Error] {e}")
#             return False

#     async def close(self):
#         """Must be called on app shutdown to clean up the pool"""
#         await self.client.aclose()


import hashlib
import httpx
from typing import Optional, Dict, Any
from configs.config_manager import cfg
from fastapi import HTTPException
from models.schemas import SiteProfile

class CacheClient:
    def __init__(self):
        # 1. Trust the config manager to have resolved the ENV var
        self.base_url = str(cfg.services['cache'].url).strip("\"' ")

        # 2. Safety check for protocol
        if not self.base_url.startswith(('http://', 'https://')):
            self.base_url = f"http://{self.base_url}"
        
        self.timeout = cfg.services['cache'].timeout
        print(f"🚀 CacheClient Initialized at: {self.base_url}")
        
        # 3. Setup persistent client with the same limits as your other layers
        self.client = httpx.AsyncClient(
            base_url=self.base_url.rstrip('/'),
            timeout=5.0,  # Slightly higher than 2.0 to avoid race conditions
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        )
        print(f"✅ CacheClient Initialized with URL: {self.base_url}")

    @staticmethod
    def compute_hash(text: str) -> str:
        """Computes a normalized SHA-256 hash of document text."""
        if not text:
            return ""
        normalized = " ".join(text.strip().split()).lower()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    async def get_by_hash(self, content_hash: str) -> Optional[SiteProfile]:
        """Fetch cached SiteProfile by document SHA-256 content hash."""
        endpoint = "/cache/site-profile/by-hash"
        try:
            response = await self.client.get(endpoint, params={"content_hash": content_hash})
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            data = response.json()
            
            profile_dict = data.get("site_profile")
            
            if data.get("found") and profile_dict:
                return SiteProfile(**profile_dict)
                
            return None
            
        except Exception as e:
            print(f"Gateway connection/validation error (Cache GET by Hash): {str(e)}")
            return None

    async def set_by_hash(self, domain: str, content_hash: str, site_profile: Any) -> bool:
        """Store SiteProfile in cache mapped to its content hash and domain."""
        try:
            if hasattr(site_profile, "model_dump") and callable(site_profile.model_dump):
                data = site_profile.model_dump()
            elif hasattr(site_profile, "dict") and callable(site_profile.dict):
                data = site_profile.dict()
            else:
                data = site_profile

            payload = {
                "domain": domain,
                "content_hash": content_hash,
                "site_profile": data
            }
            
            res = await self.client.post("/cache/site-profile", json=payload)
            return res.status_code == 200
            
        except Exception as e:
            print(f"[CacheClient SET by Hash Error] {e}")
            return False

    async def get(self, domain: str) -> Optional[SiteProfile]:
        """Legacy URL/Domain based GET lookup."""
        endpoint = "/cache/site-profile"
        try:
            response = await self.client.get(endpoint, params={"domain": domain})
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            data = response.json()
            
            profile_dict = data.get("site_profile")
            
            if data.get("found") and profile_dict:
                return SiteProfile(**profile_dict)
                
            return None
            
        except Exception as e:
            print(f"Gateway connection/validation error (Cache GET): {str(e)}")
            return None
        
    async def set(self, domain: str, site_profile: Any) -> bool:
        """Legacy URL/Domain based SET storing."""
        try:
            if hasattr(site_profile, "model_dump") and callable(site_profile.model_dump):
                data = site_profile.model_dump()
            elif hasattr(site_profile, "dict") and callable(site_profile.dict):
                data = site_profile.dict()
            else:
                data = site_profile

            payload = {
                "domain": domain, 
                "site_profile": data
            }
            
            res = await self.client.post("/cache/site-profile", json=payload)
            return res.status_code == 200
            
        except Exception as e:
            print(f"[CacheClient SET Error] {e}")
            return False

    async def close(self):
        """Must be called on app shutdown to clean up the pool"""
        await self.client.aclose()
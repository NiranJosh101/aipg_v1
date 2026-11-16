# # app/utils/db.py
# from motor.motor_asyncio import AsyncIOMotorClient
# from configs.config_manager import cfg
# from models.schemas import UserPersona


# class DatabaseManager:
#     async def get_persona_by_id(self, user_id: str) -> UserPersona:
#         # MOCK DATA: Skip the real DB and return a default persona
#         return UserPersona(
#             userId=user_id,
#             persona="Balanced",
#             constraints={
#                 "no_sharing": True,
#                 "no_tracking": True,
#                 "no_fingerprinting": False,
#                 "no_ads": True,
#                 "max_retention_30": True,
#                 "require_encryption": True,
#                 "no_location": False,
#                 "no_biometrics": False
#             }
#         )

# db = DatabaseManager()

# from datetime import datetime
# from typing import Optional, Dict, Any, List
# from motor.motor_asyncio import AsyncIOMotorClient
# from configs.config_manager import cfg
# from models.schemas import UserPersona


# class DatabaseManager:
#     def __init__(self):
#         # Initialize Motor client using MongoDB URI from config if present
#         mongo_uri = getattr(cfg, "mongo_uri", None) or "mongodb://localhost:27017"
#         self.client = AsyncIOMotorClient(mongo_uri)
#         self.db = self.client["db_josh"]
#         self.audits_collection = self.db["scan_audits"]

#     async def get_persona_by_id(self, user_id: str) -> UserPersona:
#         # MOCK DATA: Skip the real DB and return a default persona
#         return UserPersona(
#             userId=user_id,
#             persona="Balanced",
#             constraints={
#                 "no_sharing": True,
#                 "no_tracking": True,
#                 "no_fingerprinting": False,
#                 "no_ads": True,
#                 "max_retention_30": True,
#                 "require_encryption": True,
#                 "no_location": False,
#                 "no_biometrics": False
#             }
#         )

#     async def save_scan_audit(self, audit_record: Dict[str, Any]) -> bool:
#         """Saves a complete scan audit record into db-josh."""
#         try:
#             # Ensure timestamp exists
#             if "created_at" not in audit_record:
#                 audit_record["created_at"] = datetime.utcnow().isoformat()

#             # Insert into MongoDB
#             await self.audits_collection.insert_one(audit_record)
#             print(f"✅ [db-josh] Saved scan audit record for job: {audit_record.get('job_id')}")
#             return True
#         except Exception as e:
#             print(f"⚠️ [db-josh Error] Failed to save scan audit record: {e}")
#             return False

#     async def get_scan_audit(self, job_id: str) -> Optional[Dict[str, Any]]:
#         """Fetches a single audit record by job_id from db-josh."""
#         try:
#             record = await self.audits_collection.find_one({"job_id": job_id}, {"_id": 0})
#             return record
#         except Exception as e:
#             print(f"⚠️ [db-josh Error] Failed to fetch scan audit for job {job_id}: {e}")
#             return None

#     async def get_audits_by_user(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
#         """Fetches historical scan audits for a specific user from db-josh."""
#         try:
#             cursor = self.audits_collection.find({"user_id": user_id}, {"_id": 0}).sort("created_at", -1).limit(limit)
#             return await cursor.to_list(length=limit)
#         except Exception as e:
#             print(f"⚠️ [db-josh Error] Failed to fetch user audits: {e}")
#             return []


# db = DatabaseManager()

import json
import ssl
from datetime import datetime
from typing import Optional, Dict, Any, List
import asyncpg
from configs.config_manager import cfg
from models.schemas import UserPersona


class DatabaseManager:
    def __init__(self):
        # Example DSN format from Neon console:
        # postgresql://user:password@ep-cool-name-123456-pooler.us-east-2.aws.neon.tech/db_josh?sslmode=require
        self.dsn = getattr(cfg, "postgres_dsn", None) or "postgresql://user:pass@ep-example-pooler.us-east-1.aws.neon.tech/db_josh?sslmode=require"
        self.pool: Optional[asyncpg.Pool] = None

    async def init_pool(self):
        """Creates an asyncpg pool compatible with Neon's SSL requirement."""
        if not self.pool:
            # Create SSL context required by Neon
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                ssl=ssl_ctx,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            print("✅ [db-josh] Neon Postgres Connection Pool Initialized.")

    async def get_persona_by_id(self, user_id: str) -> UserPersona:
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT persona, constraints FROM user_personas WHERE user_id = $1", 
                        user_id
                    )
                    if row:
                        constraints = json.loads(row["constraints"]) if isinstance(row["constraints"], str) else row["constraints"]
                        return UserPersona(
                            userId=user_id,
                            persona=row["persona"],
                            constraints=constraints
                        )
            except Exception as e:
                print(f"⚠️ [db-josh Error] Persona lookup failed, falling back to mock: {e}")

        # Fallback Mock Data
        return UserPersona(
            userId=user_id,
            persona="Balanced",
            constraints={
                "no_sharing": True,
                "no_tracking": True,
                "no_fingerprinting": False,
                "no_ads": True,
                "max_retention_30": True,
                "require_encryption": True,
                "no_location": False,
                "no_biometrics": False
            }
        )

    async def save_scan_audit(self, audit_record: Dict[str, Any]) -> bool:
        if not self.pool:
            await self.init_pool()

        query = """
            INSERT INTO scan_audits (
                job_id, user_id, site_url, content_hash, 
                user_constraints, site_profile, verdict, created_at
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8)
            ON CONFLICT (job_id) DO UPDATE SET
                verdict = EXCLUDED.verdict,
                created_at = EXCLUDED.created_at;
        """
        try:
            created_at = datetime.fromisoformat(audit_record["created_at"]) if audit_record.get("created_at") else datetime.utcnow()
            
            async with self.pool.acquire() as conn:
                await conn.execute(
                    query,
                    audit_record["job_id"],
                    audit_record["user_id"],
                    audit_record["site_url"],
                    audit_record["content_hash"],
                    json.dumps(audit_record.get("user_constraints", {})),
                    json.dumps(audit_record.get("site_profile", {})),
                    json.dumps(audit_record.get("verdict")) if audit_record.get("verdict") else None,
                    created_at
                )
            print(f"✅ [db-josh] Saved scan audit record to Neon for job: {audit_record.get('job_id')}")
            return True
        except Exception as e:
            print(f"⚠️ [db-josh Error] Failed to save scan audit record: {e}")
            return False

    async def get_scan_audit(self, job_id: str) -> Optional[Dict[str, Any]]:
        if not self.pool:
            await self.init_pool()

        query = "SELECT job_id, user_id, site_url, content_hash, user_constraints, site_profile, verdict, created_at FROM scan_audits WHERE job_id = $1"
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(query, job_id)
                if row:
                    record = dict(row)
                    record["user_constraints"] = json.loads(record["user_constraints"]) if isinstance(record["user_constraints"], str) else record["user_constraints"]
                    record["site_profile"] = json.loads(record["site_profile"]) if isinstance(record["site_profile"], str) else record["site_profile"]
                    record["verdict"] = json.loads(record["verdict"]) if record["verdict"] and isinstance(record["verdict"], str) else record["verdict"]
                    record["created_at"] = record["created_at"].isoformat()
                    return record
            return None
        except Exception as e:
            print(f"⚠️ [db-josh Error] Failed to fetch scan audit: {e}")
            return None

    async def get_audits_by_user(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        if not self.pool:
            await self.init_pool()

        query = """
            SELECT job_id, user_id, site_url, content_hash, user_constraints, site_profile, verdict, created_at
            FROM scan_audits 
            WHERE user_id = $1 
            ORDER BY created_at DESC 
            LIMIT $2
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(query, user_id, limit)
                results = []
                for row in rows:
                    rec = dict(row)
                    rec["user_constraints"] = json.loads(rec["user_constraints"]) if isinstance(rec["user_constraints"], str) else rec["user_constraints"]
                    rec["site_profile"] = json.loads(rec["site_profile"]) if isinstance(rec["site_profile"], str) else rec["site_profile"]
                    rec["verdict"] = json.loads(rec["verdict"]) if rec["verdict"] and isinstance(rec["verdict"], str) else rec["verdict"]
                    rec["created_at"] = rec["created_at"].isoformat()
                    results.append(rec)
                return results
        except Exception as e:
            print(f"⚠️ [db-josh Error] Failed to fetch user audits: {e}")
            return []

    async def close(self):
        if self.pool:
            await self.pool.close()


db = DatabaseManager()
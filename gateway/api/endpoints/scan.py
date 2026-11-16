# # api/endpoints/scan.py
# from fastapi import APIRouter, BackgroundTasks, HTTPException, Request  # Added Request
# from models.schemas import ScanRequest, ScanStatusResponse, ScanStage
# from services.explorer_client import ExplorerClient
# from services.interpreter_client import InterpreterClient
# from services.judge_client import JudgeClient
# from services.cache_client import CacheClient
# from utils.db import db 
# from security import limiter  # Import your lean security strategy
# import uuid

# router = APIRouter()

# # Clients Instance
# explorer = ExplorerClient()
# interpreter = InterpreterClient()
# judge = JudgeClient()
# cache = CacheClient()

# # In-memory job store
# jobs = {}

# @router.post("/scan", response_model=ScanStatusResponse)
# @limiter.limit("5/minute")
# async def start_scan(request: Request, payload: ScanRequest, background_tasks: BackgroundTasks):
#     job_id = str(uuid.uuid4())
    
#     jobs[job_id] = {
#         "jobId": job_id,
#         "status": ScanStage.DISCOVERY,
#         "result": None
#     }
    
#     background_tasks.add_task(
#         run_orchestration_chain, 
#         job_id, 
#         payload.userId, 
#         payload.url
#     )
    
#     return jobs[job_id]

# def sanitize_url(url: str) -> str:
#     """Ensures the URL has a protocol prefix."""
#     if not url.startswith(('http://', 'https://')):
#         return f"http://{url}"
#     return url


# async def run_orchestration_chain(job_id: str, user_id: str, url: str):
#     try:
#         clean_url = sanitize_url(url)

#         # 1. Persona (always needed)
#         persona = await db.get_persona_by_id(user_id)
#         if not persona:
#             jobs[job_id]["status"] = ScanStage.IDLE
#             return

#         # 2. 🔥 CACHE CHECK
#         site_profile = await cache.get(clean_url)

#         if site_profile:
#             print("CACHE HIT 🚀")
#             jobs[job_id]["status"] = ScanStage.JUDGING
#             final_verdict = await judge.generate_verdict(persona, site_profile)
#             jobs[job_id]["result"] = final_verdict
#             jobs[job_id]["status"] = ScanStage.COMPLETE
#             return

#         # -------------------------
#         # CACHE MISS PATH
#         # -------------------------
#         jobs[job_id]["status"] = ScanStage.DISCOVERY
#         explorer_data = await explorer.discover_site_content(clean_url)

#         jobs[job_id]["status"] = ScanStage.REASONING
#         site_profile = await interpreter.extract_site_profile(explorer_data)

#         # store new knowledge
#         await cache.set(clean_url, site_profile)

#         # judge always runs
#         jobs[job_id]["status"] = ScanStage.JUDGING
#         final_verdict = await judge.generate_verdict(persona, site_profile)

#         jobs[job_id]["result"] = final_verdict
#         jobs[job_id]["status"] = ScanStage.COMPLETE

#     except Exception as e:
#         print(f"Pipeline Error for job {job_id}: {str(e)}")
#         jobs[job_id]["status"] = ScanStage.IDLE

# @router.get("/status/{job_id}", response_model=ScanStatusResponse)
# async def get_scan_status(job_id: str):
#     if job_id not in jobs:
#         raise HTTPException(status_code=404, detail="Job not found")
#     return jobs[job_id]

import datetime
import uuid
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from models.schemas import ScanRequest, ScanStatusResponse, ScanStage, SiteProfile
from services.explorer_client import ExplorerClient
from services.interpreter_client import InterpreterClient
from services.judge_client import JudgeClient
from services.cache_client import CacheClient
from utils.db import db 
from security import limiter

router = APIRouter()

# Clients Instance
explorer = ExplorerClient()
interpreter = InterpreterClient()
judge = JudgeClient()
cache = CacheClient()

# In-memory job store
jobs = {}

@router.post("/scan", response_model=ScanStatusResponse)
@limiter.limit("5/minute")
async def start_scan(request: Request, payload: ScanRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    
    jobs[job_id] = {
        "jobId": job_id,
        "status": ScanStage.DISCOVERY,
        "result": None
    }
    
    background_tasks.add_task(
        run_orchestration_chain, 
        job_id, 
        payload.userId, 
        payload.url
    )
    
    return jobs[job_id]

def sanitize_url(url: str) -> str:
    """Ensures the URL has a protocol prefix."""
    if not url.startswith(('http://', 'https://')):
        return f"http://{url}"
    return url


async def run_orchestration_chain(job_id: str, user_id: str, url: str):
    try:
        clean_url = sanitize_url(url)

        # 1. Fetch User Persona & Constraints
        persona = await db.get_persona_by_id(user_id)
        if not persona:
            jobs[job_id]["status"] = ScanStage.FAILED
            return

        # 2. DISCOVERY: Always scrape the site content first
        jobs[job_id]["status"] = ScanStage.DISCOVERY
        explorer_data = await explorer.discover_site_content(clean_url)
        raw_text = explorer_data.final_report

        # 3. COMPUTE CONTENT HASH
        content_hash = CacheClient.compute_hash(raw_text)

        # 4. SMART CACHE LOOKUP BY HASH
        site_profile = await cache.get_by_hash(content_hash)

        if site_profile:
            print(f"CACHE HIT 🚀 (Content Unchanged, Hash: {content_hash[:8]})")
        else:
            print(f"CACHE MISS 🔄 (New Content Detected, Hash: {content_hash[:8]})")
            jobs[job_id]["status"] = ScanStage.REASONING
            
            # Run LLM Interpreter to extract SiteProfile
            site_profile = await interpreter.extract_site_profile(explorer_data)
            
            # Attach top-level content hash
            if hasattr(site_profile, "raw_content_hash"):
                site_profile.raw_content_hash = content_hash

            # Cache the extracted profile mapped to this content hash
            await cache.set_by_hash(clean_url, content_hash, site_profile)

        # 5. JUDGE: Run compliance evaluation
        jobs[job_id]["status"] = ScanStage.JUDGING
        final_verdict = await judge.generate_verdict(persona, site_profile)

        # 6. LOG COMPLETE AUDIT RECORD TO DB-JOSH
        scan_audit_record = {
            "job_id": job_id,
            "user_id": user_id,
            "site_url": clean_url,
            "content_hash": content_hash,
            "user_constraints": persona.model_dump() if hasattr(persona, "model_dump") else persona.dict(),
            "site_profile": site_profile.model_dump() if hasattr(site_profile, "model_dump") else site_profile.dict(),
            "verdict": final_verdict.model_dump() if hasattr(final_verdict, "model_dump") else final_verdict.dict(),
            "created_at": datetime.datetime.utcnow().isoformat()
        }
        await db.save_scan_audit(scan_audit_record)

        # 7. UPDATE JOB STATE
        jobs[job_id]["result"] = final_verdict
        jobs[job_id]["status"] = ScanStage.COMPLETE

    except Exception as e:
        print(f"Pipeline Error for job {job_id}: {str(e)}")
        jobs[job_id]["status"] = ScanStage.FAILED

@router.get("/status/{job_id}", response_model=ScanStatusResponse)
async def get_scan_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return jobs[job_id]
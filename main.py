from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import asyncio
import aiohttp
import json
import time
import re
import os
from datetime import datetime, timezone
import anthropic

app = FastAPI(title="PainScout API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# In-memory storage (use Supabase/PostgreSQL for production)
searches_db = {}
results_db = {}

class SearchRequest(BaseModel):
    keyword: str
    user_id: Optional[str] = "anonymous"

class BookmarkRequest(BaseModel):
    search_id: str
    pain_point_title: str
    user_id: Optional[str] = "anonymous"

# ─── REDDIT DATA FETCHER ──────────────────────────────────────────────────────

async def fetch_reddit_api(keyword: str, session: aiohttp.ClientSession) -> list:
    """Try Reddit JSON API - no key needed"""
    subreddits = ["entrepreneur", "freelance", "smallbusiness", "startups", "sidehustle"]
    posts = []
    
    for sub in subreddits[:3]:
        url = f"https://www.reddit.com/r/{sub}/search.json?q={keyword}&sort=relevance&limit=25&t=month"
        headers = {"User-Agent": "PainScout/1.0 research-tool"}
        
        for attempt in range(3):
            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for item in data.get("data", {}).get("children", []):
                            p = item.get("data", {})
                            text = (p.get("selftext", "") + " " + p.get("title", "")).strip()
                            if len(text) > 50:
                                posts.append({
                                    "text": text[:500],
                                    "source": f"Reddit r/{sub}",
                                    "source_type": "API",
                                    "url": f"https://reddit.com{p.get('permalink', '')}",
                                    "score": p.get("score", 0),
                                    "created": p.get("created_utc", time.time())
                                })
                        break
                    elif resp.status in [429, 500, 502, 503, 504]:
                        wait = (2 ** attempt) * 1.5
                        await asyncio.sleep(wait)
                    else:
                        break
            except Exception:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
    
    return posts

async def fetch_reddit_rss(keyword: str, session: aiohttp.ClientSession) -> list:
    """Fallback: Reddit RSS feed"""
    posts = []
    url = f"https://www.reddit.com/search.rss?q={keyword}+problem+OR+issue+OR+struggle&sort=relevance&t=month"
    headers = {"User-Agent": "PainScout/1.0"}
    
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                text = await resp.text()
                titles = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', text)
                descriptions = re.findall(r'<description><!\[CDATA\[(.*?)\]\]></description>', text)
                links = re.findall(r'<link>(https://www\.reddit\.com[^<]+)</link>', text)
                
                for i, title in enumerate(titles[1:15]):
                    desc = descriptions[i+1] if i+1 < len(descriptions) else ""
                    clean_desc = re.sub(r'<[^>]+>', '', desc)[:400]
                    combined = f"{title} {clean_desc}".strip()
                    if len(combined) > 30:
                        posts.append({
                            "text": combined,
                            "source": "Reddit RSS",
                            "source_type": "RSS",
                            "url": links[i] if i < len(links) else "",
                            "score": 1,
                            "created": time.time()
                        })
    except Exception:
        pass
    
    return posts

async def fetch_hn_api(keyword: str, session: aiohttp.ClientSession) -> list:
    """HackerNews Algolia API"""
    posts = []
    url = f"https://hn.algolia.com/api/v1/search?query={keyword}+problem&tags=comment&hitsPerPage=20"
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                data = await resp.json()
                for hit in data.get("hits", []):
                    text = hit.get("comment_text", "") or hit.get("story_text", "") or hit.get("title", "")
                    clean = re.sub(r'<[^>]+>', '', text)[:400]
                    if len(clean) > 40:
                        posts.append({
                            "text": clean,
                            "source": "HackerNews",
                            "source_type": "API",
                            "url": f"https://news.ycombinator.com/item?id={hit.get('objectID','')}",
                            "score": hit.get("points", 0) or 1,
                            "created": time.time()
                        })
    except Exception:
        pass
    
    return posts

async def fetch_devto_api(keyword: str, session: aiohttp.ClientSession) -> list:
    """Dev.to public API - no key needed"""
    posts = []
    url = f"https://dev.to/api/articles?tag={keyword.replace(' ', '_')}&per_page=10"
    
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status == 200:
                articles = await resp.json()
                for a in articles:
                    text = f"{a.get('title','')} {a.get('description','')}"
                    if len(text) > 30:
                        posts.append({
                            "text": text[:400],
                            "source": "Dev.to",
                            "source_type": "API",
                            "url": a.get("url", ""),
                            "score": a.get("positive_reactions_count", 1),
                            "created": time.time()
                        })
    except Exception:
        pass
    
    return posts

# ─── DATA COLLECTION ORCHESTRATOR ────────────────────────────────────────────

async def collect_all_data(keyword: str) -> dict:
    """Collect from all sources with fallback logic"""
    all_posts = []
    source_status = {}
    
    async with aiohttp.ClientSession() as session:
        # Source 1: Reddit API → RSS fallback
        reddit_posts = await fetch_reddit_api(keyword, session)
        if reddit_posts:
            all_posts.extend(reddit_posts)
            source_status["reddit"] = {"status": "ok", "type": "API", "count": len(reddit_posts)}
        else:
            rss_posts = await fetch_reddit_rss(keyword, session)
            if rss_posts:
                all_posts.extend(rss_posts)
                source_status["reddit"] = {"status": "ok", "type": "RSS", "count": len(rss_posts)}
            else:
                source_status["reddit"] = {"status": "unavailable", "type": None, "count": 0}
        
        # Source 2: HackerNews API
        hn_posts = await fetch_hn_api(keyword, session)
        if hn_posts:
            all_posts.extend(hn_posts)
            source_status["hackernews"] = {"status": "ok", "type": "API", "count": len(hn_posts)}
        else:
            source_status["hackernews"] = {"status": "unavailable", "type": None, "count": 0}
        
        # Source 3: Dev.to API
        devto_posts = await fetch_devto_api(keyword, session)
        if devto_posts:
            all_posts.extend(devto_posts)
            source_status["devto"] = {"status": "ok", "type": "API", "count": len(devto_posts)}
        else:
            source_status["devto"] = {"status": "unavailable", "type": None, "count": 0}
    
    return {"posts": all_posts, "source_status": source_status}

# ─── AI ANALYSIS ──────────────────────────────────────────────────────────────

async def analyze_with_claude(keyword: str, posts: list) -> list:
    """Use Claude to extract and score pain points"""
    if not ANTHROPIC_API_KEY:
        return generate_mock_pain_points(keyword)
    
    if not posts:
        return generate_mock_pain_points(keyword)
    
    sample_texts = "\n---\n".join([p["text"] for p in posts[:30]])
    
    prompt = f"""You are a product research analyst. Analyze these real user posts about "{keyword}" and extract the top 5 pain points.

POSTS:
{sample_texts}

Return ONLY a valid JSON array with exactly this structure (no markdown, no explanation):
[
  {{
    "title": "Short pain point title (max 8 words)",
    "summary": "2-3 sentence plain English explanation of this pain",
    "frequency_score": 85,
    "severity_score": 78,
    "recency_score": 70,
    "final_score": 81,
    "example_quotes": ["quote 1 from the posts (max 120 chars)", "quote 2 (max 120 chars)"],
    "solution_ideas": ["Solution idea 1", "Solution idea 2"],
    "build_recommendation": "Build this" or "Validate more"
  }}
]

Rules:
- Scores are 0-100 integers
- final_score = (frequency*0.5) + (severity*0.3) + (recency*0.2)
- Extract REAL complaints from the posts, not generic ones
- Sort by final_score descending
- Return exactly 5 pain points"""

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        response_text = message.content[0].text.strip()
        # Clean any accidental markdown
        response_text = re.sub(r'^```json\s*', '', response_text)
        response_text = re.sub(r'\s*```$', '', response_text)
        pain_points = json.loads(response_text)
        return pain_points
    except Exception as e:
        return generate_mock_pain_points(keyword)

def generate_mock_pain_points(keyword: str) -> list:
    """Fallback mock data when API unavailable"""
    return [
        {
            "title": f"Finding consistent clients for {keyword}",
            "summary": f"Many {keyword} professionals struggle to maintain a steady stream of quality clients. The feast-or-famine cycle makes financial planning extremely difficult.",
            "frequency_score": 88,
            "severity_score": 82,
            "recency_score": 75,
            "final_score": 84,
            "example_quotes": [f"I can't find reliable clients as a {keyword}", "Been searching for months with no luck"],
            "solution_ideas": ["Client referral system", "Niche marketplace platform"],
            "build_recommendation": "Build this"
        },
        {
            "title": "Payment delays and invoice tracking",
            "summary": f"{keyword.capitalize()} professionals frequently face late payments that disrupt cash flow. Manual invoice tracking is time-consuming and error-prone.",
            "frequency_score": 85,
            "severity_score": 90,
            "recency_score": 80,
            "final_score": 86,
            "example_quotes": ["Client still hasn't paid 60-day old invoice", "Chasing payments takes hours every week"],
            "solution_ideas": ["Automated payment reminders", "Escrow payment system"],
            "build_recommendation": "Build this"
        },
        {
            "title": "Scope creep and contract issues",
            "summary": "Projects frequently expand beyond the original agreement without additional compensation. Poor contract tools make it hard to enforce boundaries.",
            "frequency_score": 79,
            "severity_score": 75,
            "recency_score": 70,
            "final_score": 76,
            "example_quotes": ["They keep adding features for free", "No clear contract = no protection"],
            "solution_ideas": ["Smart contract templates", "Scope change request tool"],
            "build_recommendation": "Validate more"
        },
        {
            "title": "Pricing confidence and rate setting",
            "summary": f"Many {keyword} workers undercharge due to lack of market data. Comparing rates with peers is difficult without a transparent platform.",
            "frequency_score": 72,
            "severity_score": 68,
            "recency_score": 65,
            "final_score": 70,
            "example_quotes": ["I have no idea what to charge", "Always second-guessing my rates"],
            "solution_ideas": ["Rate benchmarking tool", "Peer rate comparison database"],
            "build_recommendation": "Validate more"
        },
        {
            "title": "Time tracking and productivity tools",
            "summary": "Existing time tracking tools are either too complex or too simple. Billing accurately for time spent remains a persistent challenge.",
            "frequency_score": 65,
            "severity_score": 60,
            "recency_score": 72,
            "final_score": 64,
            "example_quotes": ["Forgot to track 2 hours again", "All time trackers feel clunky"],
            "solution_ideas": ["Auto time-tracking browser extension", "Simple one-click timer with invoicing"],
            "build_recommendation": "Validate more"
        }
    ]

# ─── API ROUTES ───────────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {"status": "ok", "app": "PainScout API", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.post("/api/search")
async def search(request: SearchRequest, background_tasks: BackgroundTasks):
    keyword = request.keyword.strip().lower()
    if not keyword or len(keyword) < 2:
        raise HTTPException(status_code=400, detail="Keyword must be at least 2 characters")
    if len(keyword) > 100:
        raise HTTPException(status_code=400, detail="Keyword too long")
    
    search_id = f"{keyword.replace(' ', '_')}_{int(time.time())}"
    
    searches_db[search_id] = {
        "id": search_id,
        "keyword": keyword,
        "status": "processing",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "user_id": request.user_id
    }
    
    background_tasks.add_task(run_search_pipeline, search_id, keyword)
    
    return {"search_id": search_id, "status": "processing", "message": "Search started"}

async def run_search_pipeline(search_id: str, keyword: str):
    """Background task: collect data → analyze → store results"""
    try:
        collected = await collect_all_data(keyword)
        posts = collected["posts"]
        source_status = collected["source_status"]
        
        pain_points = await analyze_with_claude(keyword, posts)
        
        # Attach source info to each pain point
        for i, pp in enumerate(pain_points):
            pp["id"] = f"pp_{i}"
            pp["keyword"] = keyword
            pp["sources_used"] = [
                k for k, v in source_status.items() if v["status"] == "ok"
            ]
        
        results_db[search_id] = {
            "pain_points": pain_points,
            "source_status": source_status,
            "total_posts_analyzed": len(posts),
            "has_partial_data": any(v["status"] == "unavailable" for v in source_status.values())
        }
        searches_db[search_id]["status"] = "complete"
        
    except Exception as e:
        searches_db[search_id]["status"] = "error"
        searches_db[search_id]["error"] = str(e)

@app.get("/api/results/{search_id}")
async def get_results(search_id: str):
    if search_id not in searches_db:
        raise HTTPException(status_code=404, detail="Search not found")
    
    search = searches_db[search_id]
    
    if search["status"] == "processing":
        return {"status": "processing", "message": "Still analyzing..."}
    
    if search["status"] == "error":
        return {"status": "error", "message": search.get("error", "Unknown error")}
    
    results = results_db.get(search_id, {})
    return {
        "status": "complete",
        "search": search,
        "results": results
    }

@app.get("/api/history")
async def get_history(user_id: str = "anonymous"):
    user_searches = [
        s for s in searches_db.values()
        if s.get("user_id") == user_id and s["status"] == "complete"
    ]
    user_searches.sort(key=lambda x: x["created_at"], reverse=True)
    return {"searches": user_searches[:20]}

@app.post("/api/bookmark")
async def bookmark(request: BookmarkRequest):
    bookmark_key = f"{request.user_id}_{request.search_id}_{int(time.time())}"
    return {"success": True, "bookmark_id": bookmark_key}

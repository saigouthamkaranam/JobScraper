#!/usr/bin/env python3
"""
Job Scraper Agent — Claude Haiku Only (Cheap)
Runs twice daily via GitHub Actions.
Stripped down: no cover letter, no why-you-fit.
Cost: ~$0.01-0.03 per run.
"""

import os
import re
import json
import requests
import anthropic
from datetime import date, datetime, timezone

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
NOTION_TOKEN      = os.environ.get("NOTION_TOKEN", "")
JSEARCH_API_KEY   = os.environ.get("JSEARCH_API_KEY", "")
JOB_DB_ID         = os.environ.get("JOB_DB_ID", "33b04418baad80eda498c8a42806401f")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

RESUME = """
Sai Goutham Karanam — SRE turned AI Engineer
4+ years | Python, Java, JavaScript | AWS, Azure

EXPERIENCE:
- SRE, Sam's Club: AI incident triage, REST APIs, Grafana/Splunk/Prometheus, microservices
- Software Engineer, ADP: Python pipelines, AWS ETL, CI/CD, 1.5M+ records/cycle  
- Software Engineer, Infinite Infolab: Java Spring Boot, 15+ REST APIs, MySQL, React

EDUCATION: MS Computer Science, UNC Charlotte

SKILLS: Python, Java, JavaScript, SQL, FastAPI, Spring Boot, Node.js, React,
MySQL, PostgreSQL, MongoDB, AWS, Azure, Docker, Jenkins, GitHub Actions, Terraform,
Grafana, Splunk, Prometheus, LangChain, LangGraph, RAG, OpenAI, Claude, Gemini, Ollama
"""

ROLES = [
    "AI Engineer",
    "ML Engineer", 
    "Software Engineer",
    "Full Stack Engineer",
    "Cloud Engineer",
    "SRE",
    "Platform Engineer",
]

RELEVANT_KEYWORDS = [
    "engineer", "developer", "sre", "devops", "platform", "cloud",
    "backend", "fullstack", "full stack", "ai", "ml", "machine learning",
    "software", "reliability", "infrastructure", "data engineer"
]


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def is_relevant_title(title):
    return any(kw in title.lower() for kw in RELEVANT_KEYWORDS)


def is_within_24hrs(posted_at):
    if not posted_at:
        return True
    try:
        if isinstance(posted_at, (int, float)):
            posted = datetime.fromtimestamp(posted_at, tz=timezone.utc)
        else:
            posted_at = re.sub(r"\.\d+", "", str(posted_at))
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                try:
                    posted = datetime.strptime(posted_at, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            else:
                return True
        return (datetime.now(tz=timezone.utc) - posted).total_seconds() <= 86400
    except Exception:
        return True


# ──────────────────────────────────────────────
# FETCH JOBS
# ──────────────────────────────────────────────
def fetch_jsearch(role):
    if not JSEARCH_API_KEY:
        return []
    try:
        r = requests.get(
            "https://jsearch.p.rapidapi.com/search",
            headers={"X-RapidAPI-Key": JSEARCH_API_KEY, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"},
            params={"query": f"{role} USA Remote", "page": "1", "num_pages": "1", "date_posted": "today"},
            timeout=10,
        )
        if r.status_code != 200:
            print(f"  ⚠️  JSearch error {r.status_code}")
            return []
        results = []
        for j in r.json().get("data", []):
            title = j.get("job_title", "")
            if not is_relevant_title(title):
                continue
            mn, mx = j.get("job_min_salary"), j.get("job_max_salary")
            period = j.get("job_salary_period", "")
            salary = f"${int(mn):,} - ${int(mx):,} {period}" if mn and mx else "Not specified"
            results.append({
                "title":       title,
                "company":     j.get("employer_name", ""),
                "location":    j.get("job_city", "") or ("Remote" if j.get("job_is_remote") else "USA"),
                "description": j.get("job_description", "")[:2000],
                "url":         j.get("job_apply_link", j.get("job_google_link", "")),
                "salary":      salary,
                "source":      "JSearch",
                "posted_at":   j.get("job_posted_at_datetime_utc", ""),
            })
        return results
    except Exception as e:
        print(f"  ⚠️  JSearch exception: {e}")
        return []


def fetch_himalayas(role):
    try:
        r = requests.get(
            "https://himalayas.app/jobs/api",
            params={"q": role, "limit": "20", "location": "United States"},
            timeout=10,
        )
        if r.status_code != 200:
            print(f"  ⚠️  Himalayas error {r.status_code}")
            return []
        results = []
        for j in r.json().get("jobs", []):
            title = j.get("title", "")
            if not is_relevant_title(title):
                continue
            mn, mx = j.get("salaryMin"), j.get("salaryMax")
            salary = f"${int(mn):,} - ${int(mx):,}/year" if mn and mx else "Not specified"
            results.append({
                "title":       title,
                "company":     j.get("companyName", ""),
                "location":    (j.get("locationRestrictions") or ["Remote"])[0],
                "description": j.get("description", "")[:2000],
                "url":         j.get("applicationLink", j.get("url", "")),
                "salary":      salary,
                "source":      "Himalayas",
                "posted_at":   j.get("createdAt", ""),
            })
        return results
    except Exception as e:
        print(f"  ⚠️  Himalayas exception: {e}")
        return []


# ──────────────────────────────────────────────
# ANALYZE — Haiku only, minimal prompt
# ──────────────────────────────────────────────
def analyze_job(job):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""Rate this job against the candidate. Reply ONLY with valid JSON, no markdown.

JOB: {job['title']} at {job['company']}
{job['description'][:1500]}

CANDIDATE SKILLS: {RESUME}

JSON format:
{{
  "match_score": <1-100>,
  "priority": "<High|Medium|Low>",
  "tech_stack_match": "<comma separated matching skills>",
  "skill_gaps": "<missing skills or None>",
  "red_flags": "<job posting concerns or None>",
  "email_subject": "<cold apply email subject line>"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = re.sub(r"```json|```", "", message.content[0].text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"  ⚠️  Analysis failed: {e}")
        return None


# ──────────────────────────────────────────────
# DUPLICATE CHECK
# ──────────────────────────────────────────────
def get_existing_jobs():
    existing = set()
    try:
        r = requests.post(
            f"https://api.notion.com/v1/databases/{JOB_DB_ID}/query",
            headers=NOTION_HEADERS,
            json={"page_size": 100},
        )
        if r.status_code == 200:
            for page in r.json().get("results", []):
                props = page.get("properties", {})
                t = props.get("Job Title", {}).get("title", [])
                c = props.get("Company", {}).get("rich_text", [])
                title   = t[0].get("text", {}).get("content", "").lower() if t else ""
                company = c[0].get("text", {}).get("content", "").lower() if c else ""
                if title and company:
                    existing.add(f"{title}|{company}")
    except Exception as e:
        print(f"  ⚠️  Could not fetch existing jobs: {e}")
    return existing


# ──────────────────────────────────────────────
# SAVE TO NOTION
# ──────────────────────────────────────────────
def save_to_notion(job, analysis):
    def rt(text):
        return [{"text": {"content": str(text or "")[:2000]}}]

    payload = {
        "parent": {"database_id": JOB_DB_ID},
        "properties": {
            "Job Title":        {"title": rt(f"{job['title']} — {job['company']}")},
            "Company":          {"rich_text": rt(job["company"])},
            "Source":           {"select": {"name": job["source"]}},
            "Match Score":      {"number": analysis["match_score"]},
            "Priority":         {"select": {"name": analysis["priority"]}},
            "Status":           {"status": {"name": "New"}},
            "Salary Range":     {"rich_text": rt(job["salary"])},
            "Location":         {"rich_text": rt(job["location"])},
            "Tech Stack Match": {"rich_text": rt(analysis["tech_stack_match"])},
            "Skill Gaps":       {"rich_text": rt(analysis["skill_gaps"])},
            "Red Flags":        {"rich_text": rt(analysis["red_flags"])},
            "Email Subject":    {"rich_text": rt(analysis["email_subject"])},
            "Apply URL":        {"url": job["url"] or None},
            "Date Found":       {"date": {"start": date.today().isoformat()}},
        },
        "children": [
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"text": {"content": "📋 Job Description"}}]},
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": job["description"][:2000]}}]},
            },
        ],
    }

    r = requests.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=payload)
    if r.status_code == 200:
        print(f"  ✅ {analysis['priority']} | Score:{analysis['match_score']} | {job['title']} @ {job['company']}")
    else:
        print(f"  ❌ Notion error {r.status_code}: {r.text[:150]}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print(f"\n🔍 Job Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"   Model: Claude Sonnet (lean mode)")
    print("=" * 55)

    existing = get_existing_jobs()
    print(f"📂 {len(existing)} existing jobs in Notion\n")

    all_jobs = []
    for role in ROLES:
        print(f"🔎 {role}")
        jobs = fetch_jsearch(role) + fetch_himalayas(role)
        print(f"   {len(jobs)} relevant results")
        all_jobs.extend(jobs)

    fresh = [j for j in all_jobs if is_within_24hrs(j["posted_at"])]
    new_jobs, seen = [], set(existing)
    for j in fresh:
        key = f"{j['title'].lower()}|{j['company'].lower()}"
        if key not in seen:
            new_jobs.append(j)
            seen.add(key)

    print(f"\n⏱  {len(new_jobs)} new jobs after 24hr + duplicate filter")

    if not new_jobs:
        print("✅ Nothing new. Done!")
        return

    print(f"🤖 Analyzing with Haiku...\n")
    saved = skipped = 0

    for i, job in enumerate(new_jobs, 1):
        print(f"[{i}/{len(new_jobs)}] {job['title']} @ {job['company']}")

        if len(job["description"]) < 100:
            print("  ⏭  Too short")
            skipped += 1
            continue

        analysis = analyze_job(job)
        if not analysis:
            skipped += 1
            continue

        if analysis["match_score"] < 50:
            print(f"  ⏭  Low match ({analysis['match_score']})")
            skipped += 1
            continue

        save_to_notion(job, analysis)
        saved += 1

    print(f"\n{'='*55}")
    print(f"✅ {saved} saved | {skipped} skipped")


if __name__ == "__main__":
    main()

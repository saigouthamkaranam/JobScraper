#!/usr/bin/env python3
"""
Job Scraper Agent — Hybrid Gemini + Claude Sonnet
Runs twice daily (5 AM + 5 PM CST) via GitHub Actions.
- Gemini 2.0 Flash: scoring, skill gaps, red flags (free)
- Claude Sonnet: cover letters for High priority jobs only (premium quality)
- Fallback: Claude Haiku if Gemini fails
Sources: JSearch + Himalayas
"""

import os
import re
import json
import requests
import anthropic
from datetime import date, datetime, timezone

# ──────────────────────────────────────────────
# CONFIG — GitHub Actions Secrets
# ──────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")
NOTION_TOKEN      = os.environ.get("NOTION_TOKEN", "")
JSEARCH_API_KEY   = os.environ.get("JSEARCH_API_KEY", "")
JOB_DB_ID         = os.environ.get("JOB_DB_ID", "33b04418baad80eda498c8a42806401f")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ──────────────────────────────────────────────
# RESUME
# ──────────────────────────────────────────────
RESUME = """
Sai Goutham Karanam — SRE turned AI Engineer
4+ years experience | Python, Java, JavaScript | AWS, Azure

EXPERIENCE:
- Software Engineer (SRE), Sam's Club (Dec 2024 - Present)
  * AI-driven incident triage system resolving 30+ incidents daily
  * Event-driven backend automation, REST APIs consolidating 12 monitoring systems
  * Grafana, Splunk, Prometheus dashboards for 40+ microservices
  * Eliminated 8 recurring production outages via root cause analysis

- Software Engineer, ADP (Nov 2021 - Dec 2022)
  * Python data pipelines for payroll/HR platforms, 1.5M+ records/cycle
  * AWS ETL workflows, reduced batch processing from 4hrs to 90mins
  * CI/CD with Jenkins, Docker, Git

- Software Engineer, Infinite Infolab (Oct 2019 - Nov 2021)
  * Java Spring Boot backend, 15+ REST APIs
  * MySQL optimization, React-Redux UI migration

EDUCATION: MS Computer Science, UNC Charlotte (2023-2024)

SKILLS:
Languages: Python, Java, JavaScript, SQL
Backend: FastAPI, Spring Boot, Node.js, REST APIs, Microservices
Frontend: React, Redux
Databases: MySQL, PostgreSQL, MongoDB
Cloud: AWS (EC2, S3, Lambda), Azure
DevOps: Docker, Jenkins, GitHub Actions, Terraform
Monitoring: Grafana, Splunk, Prometheus
AI/ML: OpenAI, LangChain, LangGraph, Pinecone, RAG, Pydantic AI, CrewAI, Claude, Ollama, Gemini
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

RELEVANT_TITLE_KEYWORDS = [
    "engineer", "developer", "sre", "devops", "platform", "cloud",
    "backend", "fullstack", "full stack", "ai", "ml", "machine learning",
    "software", "reliability", "infrastructure", "data engineer"
]


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def is_relevant_title(title):
    title_lower = title.lower()
    return any(kw in title_lower for kw in RELEVANT_TITLE_KEYWORDS)


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
        now = datetime.now(tz=timezone.utc)
        return (now - posted).total_seconds() <= 86400
    except Exception:
        return True


def build_recruiter_search(company, role):
    query = f'site:linkedin.com/in "recruiter" OR "talent" "{company}" "{role}"'
    encoded = requests.utils.quote(query)
    return f"https://www.google.com/search?q={encoded}"


# ──────────────────────────────────────────────
# FETCH JOBS
# ──────────────────────────────────────────────
def fetch_jsearch(role):
    if not JSEARCH_API_KEY:
        print("  ⚠️  No JSEARCH_API_KEY, skipping")
        return []
    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{role} USA Remote",
        "page": "1",
        "num_pages": "1",
        "date_posted": "today",
    }
    headers = {
        "X-RapidAPI-Key": JSEARCH_API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            jobs = r.json().get("data", [])
            results = []
            for j in jobs:
                title = j.get("job_title", "")
                if not is_relevant_title(title):
                    continue
                salary = ""
                mn = j.get("job_min_salary")
                mx = j.get("job_max_salary")
                period = j.get("job_salary_period", "")
                if mn and mx:
                    salary = f"${int(mn):,} - ${int(mx):,} {period}"
                results.append({
                    "title":       title,
                    "company":     j.get("employer_name", ""),
                    "location":    j.get("job_city", "") or ("Remote" if j.get("job_is_remote") else "USA"),
                    "description": j.get("job_description", "")[:3000],
                    "url":         j.get("job_apply_link", j.get("job_google_link", "")),
                    "salary":      salary or "Not specified",
                    "source":      "JSearch",
                    "posted_at":   j.get("job_posted_at_datetime_utc", ""),
                })
            return results
        else:
            print(f"  ⚠️  JSearch error {r.status_code}")
            return []
    except Exception as e:
        print(f"  ⚠️  JSearch exception: {e}")
        return []


def fetch_himalayas(role):
    url = "https://himalayas.app/jobs/api"
    params = {"q": role, "limit": "20", "location": "United States"}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            results = []
            for j in jobs:
                title = j.get("title", "")
                if not is_relevant_title(title):
                    continue
                mn = j.get("salaryMin")
                mx = j.get("salaryMax")
                salary = f"${int(mn):,} - ${int(mx):,}/year" if mn and mx else "Not specified"
                results.append({
                    "title":       title,
                    "company":     j.get("companyName", ""),
                    "location":    j.get("locationRestrictions", ["Remote"])[0] if j.get("locationRestrictions") else "Remote",
                    "description": j.get("description", "")[:3000],
                    "url":         j.get("applicationLink", j.get("url", "")),
                    "salary":      salary,
                    "source":      "Himalayas",
                    "posted_at":   j.get("createdAt", ""),
                })
            return results
        else:
            print(f"  ⚠️  Himalayas error {r.status_code}")
            return []
    except Exception as e:
        print(f"  ⚠️  Himalayas exception: {e}")
        return []


# ──────────────────────────────────────────────
# GEMINI — scoring + analysis (free)
# ──────────────────────────────────────────────
def analyze_with_gemini(job):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"
    prompt = f"""You are an expert recruiter. Analyze this job against the candidate resume.

JOB:
Title: {job['title']}
Company: {job['company']}
Location: {job['location']}
Description: {job['description']}

RESUME:
{RESUME}

Respond ONLY with valid JSON, no markdown, no backticks:
{{
  "match_score": <integer 1-100>,
  "priority": "<High|Medium|Low>",
  "why_you_fit": "<3 bullet points starting with • why this candidate fits>",
  "skill_gaps": "<comma separated missing skills or None>",
  "tech_stack_match": "<comma separated matching skills>",
  "red_flags": "<concerns about this job posting or None>",
  "email_subject": "<compelling cold apply email subject line>"
}}"""

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    try:
        r = requests.post(url, json=payload, timeout=30)
        if r.status_code == 200:
            text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            text = re.sub(r"```json|```", "", text).strip()
            return json.loads(text), "gemini"
        else:
            print(f"  ⚠️  Gemini error {r.status_code} — falling back to Haiku")
            return None, None
    except Exception as e:
        print(f"  ⚠️  Gemini exception: {e} — falling back to Haiku")
        return None, None


# ──────────────────────────────────────────────
# CLAUDE HAIKU — fallback scoring (cheap)
# ──────────────────────────────────────────────
def analyze_with_haiku(job):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = f"""Analyze this job against the candidate resume.

JOB: {job['title']} at {job['company']}
{job['description'][:2000]}

RESUME:
{RESUME}

Respond ONLY with valid JSON, no markdown:
{{
  "match_score": <integer 1-100>,
  "priority": "<High|Medium|Low>",
  "why_you_fit": "<3 bullet points starting with • >",
  "skill_gaps": "<missing skills or None>",
  "tech_stack_match": "<matching skills>",
  "red_flags": "<concerns or None>",
  "email_subject": "<email subject line>"
}}"""
    try:
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text), "haiku"
    except Exception as e:
        print(f"  ⚠️  Haiku fallback failed: {e}")
        return None, None


# ──────────────────────────────────────────────
# CLAUDE SONNET — cover letters for High priority
# ──────────────────────────────────────────────
def generate_cover_letter_sonnet(job, analysis):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = f"""Write a compelling, personalized cover letter for this job application.

JOB: {job['title']} at {job['company']}
Location: {job['location']}
Description: {job['description'][:2000]}

CANDIDATE:
{RESUME}

WHY THEY FIT:
{analysis.get('why_you_fit', '')}

MATCHING SKILLS:
{analysis.get('tech_stack_match', '')}

Rules:
- 3 paragraphs, professional but human tone
- Reference the company specifically
- Highlight the most relevant experience for THIS role
- End with a clear CTA
- Do NOT use "I am writing to apply" or generic openers
- Sound like a real person, not a template

Output ONLY the cover letter text."""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    except Exception as e:
        print(f"  ⚠️  Sonnet cover letter failed: {e}")
        return analysis.get("why_you_fit", "See resume for qualifications.")


# ──────────────────────────────────────────────
# ANALYZE JOB — orchestrator
# ──────────────────────────────────────────────
def analyze_job(job):
    # Step 1: Score with Gemini (free)
    analysis, model_used = analyze_with_gemini(job)

    # Step 2: Fallback to Haiku if Gemini fails
    if not analysis:
        analysis, model_used = analyze_with_haiku(job)

    if not analysis:
        return None, None

    # Step 3: Generate cover letter
    if analysis.get("priority") == "High":
        # Sonnet for High priority — best quality
        print(f"  ✨ High priority — using Sonnet for cover letter")
        cover_letter = generate_cover_letter_sonnet(job, analysis)
    else:
        # Gemini/Haiku already has enough for Medium priority
        cover_letter = f"Dear Hiring Team,\n\n{analysis.get('why_you_fit', '')}\n\nI would love to discuss this opportunity further.\n\nBest regards,\nSai Goutham Karanam"

    analysis["cover_letter"] = cover_letter
    analysis["model_used"] = model_used
    return analysis, model_used


# ──────────────────────────────────────────────
# DUPLICATE CHECK
# ──────────────────────────────────────────────
def get_existing_jobs():
    existing = set()
    url = f"https://api.notion.com/v1/databases/{JOB_DB_ID}/query"
    try:
        r = requests.post(url, headers=NOTION_HEADERS, json={"page_size": 100})
        if r.status_code == 200:
            for page in r.json().get("results", []):
                props = page.get("properties", {})
                t = props.get("Job Title", {}).get("title", [])
                c = props.get("Company", {}).get("rich_text", [])
                title = t[0].get("text", {}).get("content", "").lower() if t else ""
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
    today = date.today().isoformat()
    title = f"{job['title']} — {job['company']}"

    def rt(text):
        return [{"text": {"content": str(text or "")[:2000]}}]

    payload = {
        "parent": {"database_id": JOB_DB_ID},
        "properties": {
            "Job Title":        {"title": rt(title)},
            "Company":          {"rich_text": rt(job["company"])},
            "Source":           {"select": {"name": job["source"]}},
            "Match Score":      {"number": analysis["match_score"]},
            "Priority":         {"select": {"name": analysis["priority"]}},
            "Status":           {"status": {"name": "New"}},
            "Salary Range":     {"rich_text": rt(job["salary"])},
            "Location":         {"rich_text": rt(job["location"])},
            "Tech Stack Match": {"rich_text": rt(analysis["tech_stack_match"])},
            "Why You Fit":      {"rich_text": rt(analysis["why_you_fit"])},
            "Skill Gaps":       {"rich_text": rt(analysis["skill_gaps"])},
            "Red Flags":        {"rich_text": rt(analysis["red_flags"])},
            "Cover Letter":     {"rich_text": rt(analysis["cover_letter"])},
            "Email Subject":    {"rich_text": rt(analysis["email_subject"])},
            "Apply URL":        {"url": job["url"] or None},
            "Recruiter Search": {"url": build_recruiter_search(job["company"], job["title"])},
            "Date Found":       {"date": {"start": today}},
        },
        "children": [
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [{"text": {"content": f"Analyzed by: {analysis.get('model_used', 'AI').upper()} | Score: {analysis['match_score']}/100 | Priority: {analysis['priority']}"}}],
                    "icon": {"emoji": "🤖"},
                }
            },
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"text": {"content": "✉️ Cover Letter"}}]},
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": analysis["cover_letter"][:2000]}}]},
            },
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
        model_tag = "✨ Sonnet" if analysis.get("model_used") == "gemini" and analysis.get("priority") == "High" else f"🤖 {analysis.get('model_used', '').title()}"
        print(f"  ✅ Saved [{model_tag}] Score:{analysis['match_score']} | {analysis['priority']} | {title}")
    else:
        print(f"  ❌ Notion error {r.status_code}: {r.text[:200]}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print(f"\n🔍 Job Scraper starting — {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC")
    print(f"   Mode: Gemini (scoring) + Sonnet (cover letters) + Haiku (fallback)")
    print("=" * 60)

    print("\n📂 Fetching existing jobs from Notion...")
    existing = get_existing_jobs()
    print(f"   Found {len(existing)} existing jobs")

    all_jobs = []
    for role in ROLES:
        print(f"\n🔎 Searching: {role}")
        jsearch_jobs   = fetch_jsearch(role)
        himalayas_jobs = fetch_himalayas(role)
        found = jsearch_jobs + himalayas_jobs
        print(f"   Found {len(found)} relevant jobs")
        all_jobs.extend(found)

    print(f"\n⏱  Filtering to last 24 hours...")
    fresh = [j for j in all_jobs if is_within_24hrs(j["posted_at"])]

    new_jobs = []
    for j in fresh:
        key = f"{j['title'].lower()}|{j['company'].lower()}"
        if key not in existing:
            new_jobs.append(j)
            existing.add(key)

    print(f"   {len(new_jobs)} new jobs after 24hr + duplicate filter")

    if not new_jobs:
        print("\n✅ No new jobs to process. Done!")
        return

    print(f"\n🤖 Analyzing {len(new_jobs)} jobs...")
    print("=" * 60)

    saved = skipped = errors = 0
    for i, job in enumerate(new_jobs, 1):
        print(f"\n[{i}/{len(new_jobs)}] {job['title']} @ {job['company']}")

        if len(job["description"]) < 100:
            print("  ⏭  Skipping — description too short")
            skipped += 1
            continue

        analysis, model_used = analyze_job(job)

        if not analysis:
            print("  ❌ Analysis failed completely")
            errors += 1
            continue

        if analysis["match_score"] < 50:
            print(f"  ⏭  Skipping — low match ({analysis['match_score']})")
            skipped += 1
            continue

        save_to_notion(job, analysis)
        saved += 1

    print("\n" + "=" * 60)
    print(f"✅ Done! {saved} saved | {skipped} skipped | {errors} errors")
    print("=" * 60)


if __name__ == "__main__":
    main()

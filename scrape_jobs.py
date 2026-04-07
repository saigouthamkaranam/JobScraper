#!/usr/bin/env python3
"""
Job Scraper Agent
Runs twice daily (5 AM + 5 PM CST) via GitHub Actions.
Sources: JSearch (via RapidAPI) + Himalayas
Features: 24hr filter, resume match score, cover letter, skill gap, priority ranking,
          red flag detection, duplicate filter, recruiter search URL.
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
NOTION_TOKEN      = os.environ.get("NOTION_TOKEN", "")
JSEARCH_API_KEY   = os.environ.get("JSEARCH_API_KEY", "")
JOB_DB_ID         = os.environ.get("JOB_DB_ID", "33b04418baad80eda498c8a42806401f")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ──────────────────────────────────────────────
# YOUR RESUME — used for scoring + cover letters
# ──────────────────────────────────────────────
RESUME = """
Sai Goutham Karanam
Software Engineer with 4+ years of experience building scalable backend systems and cloud-native applications using Python, Java, and Azure.

EXPERIENCE:
- Software Engineer (SRE), Sam's Club (Dec 2024 - Present)
  * AI-driven incident triage system in Python, resolving 30+ incidents daily
  * Event-driven backend automation, REST APIs consolidating alerts from 12 monitoring systems
  * Grafana, Splunk, Prometheus dashboards tracking 40+ microservice health metrics
  * Root cause analysis across distributed microservices, eliminating 8 recurring production outages

- Software Engineer, ADP (Nov 2021 - Dec 2022)
  * Python backend data pipelines and RESTful services for payroll/HR platforms
  * AWS ETL workflows, reduced batch processing from 4 hours to 90 minutes
  * CI/CD pipelines using Jenkins, Docker, Git

- Software Engineer, Infinite Infolab (Oct 2019 - Nov 2021)
  * Java Spring Boot backend modules, 15+ RESTful APIs
  * MySQL optimization, React-Redux UI migration

EDUCATION: MS Computer Science, UNC Charlotte (Jan 2023 - May 2024)

SKILLS:
- Languages: Python, Java, JavaScript, SQL
- Backend: FastAPI, Spring Boot, Node.js, Express.js, REST APIs, Microservices
- Frontend: React, Redux
- Databases: MySQL, PostgreSQL, MongoDB
- Cloud: AWS (EC2, S3, IAM, Lambda), Azure
- DevOps: Docker, Jenkins, Git, GitHub Actions, Terraform
- Monitoring: Grafana, Splunk, Prometheus
- AI/ML: OpenAI APIs, LangChain, LangGraph, Pinecone, Hugging Face, RAG, Pydantic AI, CrewAI, Claude, Ollama
"""

# ──────────────────────────────────────────────
# TARGET ROLES
# ──────────────────────────────────────────────
ROLES = [
    "AI Engineer",
    "ML Engineer",
    "Software Engineer",
    "Full Stack Engineer",
    "Cloud Engineer",
    "SRE",
    "Platform Engineer",
]


# ──────────────────────────────────────────────
# FETCH JOBS
# ──────────────────────────────────────────────
def fetch_jsearch(role):
    """Fetch jobs from JSearch API (covers Indeed, Glassdoor, LinkedIn)"""
    if not JSEARCH_API_KEY:
        print("  ⚠️  No JSEARCH_API_KEY set, skipping JSearch")
        return []

    url = "https://jsearch.p.rapidapi.com/search"
    params = {
        "query": f"{role} USA Remote",
        "page": "1",
        "num_pages": "1",
        "date_posted": "today",
        "remote_jobs_only": "false",
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
                results.append({
                    "title":       j.get("job_title", ""),
                    "company":     j.get("employer_name", ""),
                    "location":    j.get("job_city", "") or ("Remote" if j.get("job_is_remote") else "USA"),
                    "description": j.get("job_description", "")[:3000],
                    "url":         j.get("job_apply_link", j.get("job_google_link", "")),
                    "salary":      _parse_jsearch_salary(j),
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


def _parse_jsearch_salary(job):
    mn = job.get("job_min_salary")
    mx = job.get("job_max_salary")
    period = job.get("job_salary_period", "")
    if mn and mx:
        return f"${int(mn):,} - ${int(mx):,} {period}"
    elif mn:
        return f"${int(mn):,}+ {period}"
    return "Not specified"


def fetch_himalayas(role):
    """Fetch jobs from Himalayas free API"""
    url = "https://himalayas.app/jobs/api"
    params = {"q": role, "limit": "10"}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            jobs = r.json().get("jobs", [])
            results = []
            for j in jobs:
                results.append({
                    "title":       j.get("title", ""),
                    "company":     j.get("companyName", ""),
                    "location":    j.get("locationRestrictions", ["Remote"])[0] if j.get("locationRestrictions") else "Remote",
                    "description": j.get("description", "")[:3000],
                    "url":         j.get("applicationLink", j.get("url", "")),
                    "salary":      _parse_himalayas_salary(j),
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


def _parse_himalayas_salary(job):
    mn = job.get("salaryMin")
    mx = job.get("salaryMax")
    if mn and mx:
        return f"${int(mn):,} - ${int(mx):,}/year"
    return "Not specified"


def is_within_24hrs(posted_at):
    """Return True if job was posted within the last 24 hours"""
    if not posted_at:
        return True  # include if unknown
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


# ──────────────────────────────────────────────
# DUPLICATE CHECK
# ──────────────────────────────────────────────
def get_existing_jobs():
    """Fetch job titles + companies already in Notion to avoid duplicates"""
    existing = set()
    url = f"https://api.notion.com/v1/databases/{JOB_DB_ID}/query"
    payload = {"page_size": 100}
    try:
        r = requests.post(url, headers=NOTION_HEADERS, json=payload)
        if r.status_code == 200:
            for page in r.json().get("results", []):
                props = page.get("properties", {})
                title = ""
                company = ""
                t = props.get("Job Title", {}).get("title", [])
                if t:
                    title = t[0].get("text", {}).get("content", "").lower()
                c = props.get("Company", {}).get("rich_text", [])
                if c:
                    company = c[0].get("text", {}).get("content", "").lower()
                if title and company:
                    existing.add(f"{title}|{company}")
    except Exception as e:
        print(f"  ⚠️  Could not fetch existing jobs: {e}")
    return existing


# ──────────────────────────────────────────────
# CLAUDE ANALYSIS
# ──────────────────────────────────────────────
def analyze_job(job):
    """Use Claude to score, write cover letter, detect red flags, etc."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    prompt = f"""You are an expert career coach and recruiter. Analyze this job posting against the candidate's resume.

JOB POSTING:
Title: {job['title']}
Company: {job['company']}
Location: {job['location']}
Description: {job['description']}

CANDIDATE RESUME:
{RESUME}

Respond ONLY with a valid JSON object (no markdown, no backticks) with these exact keys:
{{
  "match_score": <integer 1-100>,
  "priority": "<High|Medium|Low>",
  "why_you_fit": "<3 concise bullet points starting with • explaining why this candidate fits>",
  "skill_gaps": "<comma separated list of missing skills, or 'None' if no gaps>",
  "tech_stack_match": "<comma separated list of matching skills from the resume>",
  "red_flags": "<any concerns like unrealistic requirements, vague role, etc. or 'None'>",
  "cover_letter": "<a 3 paragraph personalized cover letter for this specific role and company>",
  "email_subject": "<a compelling cold apply email subject line>"
}}"""

    try:
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = message.content[0].text.strip()
        text = re.sub(r"```json|```", "", text).strip()
        return json.loads(text)
    except Exception as e:
        print(f"  ⚠️  Claude analysis failed: {e}")
        return {
            "match_score": 0,
            "priority": "Low",
            "why_you_fit": "Analysis failed",
            "skill_gaps": "Unknown",
            "tech_stack_match": "Unknown",
            "red_flags": "Unknown",
            "cover_letter": "Analysis failed",
            "email_subject": f"Application for {job['title']} at {job['company']}",
        }


def build_recruiter_search(company, role):
    query = f'site:linkedin.com/in "recruiter" OR "talent" "{company}" "{role}"'
    encoded = requests.utils.quote(query)
    return f"https://www.google.com/search?q={encoded}"


# ──────────────────────────────────────────────
# SAVE TO NOTION
# ──────────────────────────────────────────────
def save_to_notion(job, analysis):
    today = date.today().isoformat()
    title = f"{job['title']} — {job['company']}"

    def rt(text):
        text = str(text or "")[:2000]
        return [{"text": {"content": text}}]

    payload = {
        "parent": {"database_id": JOB_DB_ID},
        "properties": {
            "Job Title":        {"title": rt(title)},
            "Company":          {"rich_text": rt(job["company"])},
            "Source":           {"select": {"name": job["source"]}},
            "Match Score":      {"number": analysis["match_score"]},
            "Priority":         {"select": {"name": analysis["priority"]}},
            "Status":           {"status": {"name": "Not started"}},
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
                "type": "heading_2",
                "heading_2": {"rich_text": [{"text": {"content": "Cover Letter"}}]},
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": analysis["cover_letter"][:2000]}}]},
            },
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": [{"text": {"content": "Job Description"}}]},
            },
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"text": {"content": job["description"][:2000]}}]},
            },
        ],
    }

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json=payload,
    )
    if r.status_code == 200:
        print(f"  ✅ Saved: {title} (Score: {analysis['match_score']}, Priority: {analysis['priority']})")
    else:
        print(f"  ❌ Notion error {r.status_code}: {r.text[:200]}")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    print(f"\n🔍 Job Scraper starting — {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC")
    print("=" * 60)

    # Get existing jobs to avoid duplicates
    print("\n📂 Fetching existing jobs from Notion...")
    existing = get_existing_jobs()
    print(f"   Found {len(existing)} existing jobs")

    all_jobs = []

    # Fetch from all sources
    for role in ROLES:
        print(f"\n🔎 Searching: {role}")
        jsearch_jobs   = fetch_jsearch(role)
        himalayas_jobs = fetch_himalayas(role)
        found = jsearch_jobs + himalayas_jobs
        print(f"   Found {len(found)} jobs ({len(jsearch_jobs)} JSearch, {len(himalayas_jobs)} Himalayas)")
        all_jobs.extend(found)

    # Filter: 24hrs + deduplicate
    print(f"\n⏱  Filtering to last 24 hours...")
    fresh_jobs = [j for j in all_jobs if is_within_24hrs(j["posted_at"])]
    print(f"   {len(fresh_jobs)}/{len(all_jobs)} jobs are within 24 hours")

    new_jobs = []
    for j in fresh_jobs:
        key = f"{j['title'].lower()}|{j['company'].lower()}"
        if key not in existing:
            new_jobs.append(j)
            existing.add(key)

    print(f"   {len(new_jobs)} new jobs after duplicate filter")

    if not new_jobs:
        print("\n✅ No new jobs to process. Done!")
        return

    # Analyze + save
    print(f"\n🤖 Analyzing {len(new_jobs)} jobs with Claude...")
    print("=" * 60)

    saved = 0
    skipped = 0
    for i, job in enumerate(new_jobs, 1):
        print(f"\n[{i}/{len(new_jobs)}] {job['title']} @ {job['company']}")

        # Skip very low-info jobs
        if len(job["description"]) < 100:
            print("  ⏭  Skipping — description too short")
            skipped += 1
            continue

        analysis = analyze_job(job)

        # Only save if match score >= 40
        if analysis["match_score"] < 40:
            print(f"  ⏭  Skipping — low match score ({analysis['match_score']})")
            skipped += 1
            continue

        save_to_notion(job, analysis)
        saved += 1

    print("\n" + "=" * 60)
    print(f"✅ Done! {saved} jobs saved to Notion, {skipped} skipped")
    print("=" * 60)


if __name__ == "__main__":
    main()

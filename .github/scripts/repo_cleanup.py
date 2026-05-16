from github import Github
import os
from datetime import datetime, timedelta, timezone

# Configuration
DAYS_STALE = 30
FEATURE_DAYS_STALE = 180
EXEMPT_BRANCHES = ['main']

# Auth and repo
token = os.getenv("GITHUB_TOKEN")
repo_name = os.getenv("REPO_NAME")
g = Github(token)
repo = g.get_repo(repo_name)
print(f"Repository Cleanup 🧼")
print(f"Connected to repo {repo_name}")
print(f"Deleting `features/*` branches more than {FEATURE_DAYS_STALE} days old...")
print(f"Deleting other branches more than {DAYS_STALE} days old...")

for branch in repo.get_branches():
    branch_name = branch.name
    branch_stale_allowance = DAYS_STALE
    if branch_name in EXEMPT_BRANCHES:
        print(f"{branch_name} is exempt from deletion; skipping")
        continue
    elif branch_name.startswith('features/'):
        branch_stale_allowance = FEATURE_DAYS_STALE
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=DAYS_STALE)

    commit = repo.get_commit(branch.commit.sha)
    last_commit_date = commit.commit.author.date
    is_stale = last_commit_date < cutoff_date
    
    linked_to_issue = any(
        (branch_name in (issue.title or "") or branch_name in (issue.body or ""))
        for issue in repo.get_issues(state="open")
    )

    if is_stale and not linked_to_issue:
        print(f"{branch_name} has last commit date {last_commit_date} - deleting 🧼")
        ref = repo.get_git_ref(f"heads/{branch_name}")
        ref.delete()
    elif is_stale and linked_to_issue:
        print(f"{branch_name} has last commit date {last_commit_date} and is connected to an issue - please review ⚠️")
    else:
        print(f"{branch_name} has last commit date {last_commit_date} - retaining ✅")

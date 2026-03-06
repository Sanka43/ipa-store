"""
IPA Store Dataset Collector
Collects real iOS IPA application metadata from public GitHub repositories
using the GitHub REST API and builds a JSON database.
"""

import json
import time
import requests
from typing import Optional, Tuple

# Configuration
GITHUB_API_BASE = "https://api.github.com"
TARGET_APPS = 1000
OUTPUT_FILE = "ipa_store_dataset.json"
REQUEST_DELAY = 1.2  # seconds between requests (unauthenticated: 60/hr; authenticated: 5000/hr)
RATE_LIMIT_RETRY_DELAY = 60  # seconds to wait when rate limited

# Search keywords for iOS IPA distribution
SEARCH_KEYWORDS = [
    "ipa",
    "ios ipa",
    "sideload ios",
    "trollstore",
    "ios emulator",
    "ios app release",
]

# Optional: Set GITHUB_TOKEN env var or pass to increase rate limit (5000/hr)
GITHUB_TOKEN: Optional[str] = None  # os.environ.get("GITHUB_TOKEN")


def get_headers() -> dict:
    """Build request headers, optionally with auth token."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "IPA-Store-Collector/1.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def search_repositories(keyword: str, page: int = 1, per_page: int = 100) -> dict:
    """
    Search GitHub for repositories matching the keyword.
    Returns API response JSON or empty dict on error.
    """
    url = f"{GITHUB_API_BASE}/search/repositories"
    params = {
        "q": keyword,
        "sort": "stars",
        "order": "desc",
        "per_page": per_page,
        "page": page,
    }
    try:
        r = requests.get(url, params=params, headers=get_headers(), timeout=30)
        if r.status_code == 403:
            # Rate limited
            return {"rate_limited": True, "reset_at": r.headers.get("X-RateLimit-Reset")}
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"  [ERROR] Search failed for '{keyword}' page {page}: {e}")
        return {}


def fetch_releases(owner: str, repo: str) -> Tuple[list, bool, Optional[str]]:
    """
    Fetch all releases for a repository.
    Returns (releases_list, rate_limited, reset_at).
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/releases"
    all_releases = []
    page = 1
    while True:
        try:
            r = requests.get(
                url,
                params={"per_page": 100, "page": page},
                headers=get_headers(),
                timeout=30,
            )
            if r.status_code == 404:
                return ([], False, None)
            if r.status_code == 403:
                return ([], True, r.headers.get("X-RateLimit-Reset"))
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            all_releases.extend(data)
            if len(data) < 100:
                break
            page += 1
            time.sleep(REQUEST_DELAY)
        except requests.RequestException as e:
            print(f"  [ERROR] Releases fetch failed for {owner}/{repo}: {e}")
            break
    return (all_releases, False, None)


def extract_ipa_assets(releases: list) -> list:
    """
    From a list of release objects, extract (release, asset) pairs
    where the asset is an .ipa file.
    """
    ipa_assets = []
    for release in releases:
        assets = release.get("assets") or []
        for asset in assets:
            name = (asset.get("name") or "").lower()
            if name.endswith(".ipa"):
                ipa_assets.append((release, asset))
    return ipa_assets


def build_dataset(
    seen_repos: set,
    repo_full_name: str,
    repo_data: dict,
    releases: list,
    ipa_assets: list,
) -> list:
    """
    Build list of app record dicts from repo data, releases, and IPA assets.
    Only adds records for repos not in seen_repos; updates seen_repos.
    """
    records = []
    owner = repo_data.get("owner", {}).get("login", "")
    repo_name = repo_data.get("name", "")
    repo_url = repo_data.get("html_url", "")
    description = (repo_data.get("description") or "").strip()
    stars = repo_data.get("stargazers_count")
    updated_at = repo_data.get("updated_at", "")

    for release, asset in ipa_assets:
        app_name = (release.get("name") or release.get("tag_name") or asset.get("name") or "Unknown").strip()
        version = release.get("tag_name", "")
        ipa_url = asset.get("browser_download_url", "")
        file_size = asset.get("size")
        if file_size is not None:
            file_size_str = f"{file_size}"
        else:
            file_size_str = ""
        release_date = (release.get("published_at") or release.get("created_at") or "")

        records.append({
            "app_name": app_name,
            "developer": owner,
            "repository_name": repo_name,
            "repository_url": repo_url,
            "description": description,
            "version": version,
            "ipa_download_url": ipa_url,
            "file_size": file_size_str,
            "release_date": release_date,
            "stars": str(stars) if stars is not None else "",
            "last_updated": updated_at,
        })
    if records:
        seen_repos.add(repo_full_name)
    return records


def save_json(apps: list, filepath: str) -> None:
    """Write the apps list to a JSON file with the required structure."""
    data = {"apps": apps}
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(apps)} apps to {filepath}")


def wait_for_rate_limit(reset_at: Optional[str]) -> None:
    """Wait until rate limit resets (or default delay)."""
    if reset_at:
        try:
            reset_ts = int(reset_at)
            wait = max(5, reset_ts - int(time.time()))
            print(f"  Rate limited. Waiting {wait} seconds...")
            time.sleep(wait)
        except (ValueError, TypeError):
            time.sleep(RATE_LIMIT_RETRY_DELAY)
    else:
        time.sleep(RATE_LIMIT_RETRY_DELAY)


def main() -> None:
    global GITHUB_TOKEN
    try:
        import os
        GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
    except Exception:
        pass

    all_apps = []
    seen_repos = set()
    repos_scanned = 0
    repos_with_ipa = 0

    print("IPA Store Dataset Collector")
    print("Target: at least", TARGET_APPS, "IPA apps")
    print("Output:", OUTPUT_FILE)
    print("-" * 50)

    for keyword in SEARCH_KEYWORDS:
        if len(all_apps) >= TARGET_APPS:
            break
        page = 1
        while len(all_apps) < TARGET_APPS:
            print(f"\nSearching: '{keyword}' (page {page})...")
            result = search_repositories(keyword, page=page)

            if result.get("rate_limited"):
                wait_for_rate_limit(result.get("reset_at"))
                continue

            items = result.get("items") or []
            if not items:
                break

            for repo in items:
                if len(all_apps) >= TARGET_APPS:
                    break
                full_name = repo.get("full_name", "")
                if not full_name or full_name in seen_repos:
                    continue
                owner = repo.get("owner", {}).get("login", "")
                name = repo.get("name", "")
                if not owner or not name:
                    continue

                print(f"  Scanning repository... {full_name}")
                repos_scanned += 1
                releases, rate_limited, reset_at = fetch_releases(owner, name)
                if rate_limited:
                    wait_for_rate_limit(reset_at)
                    releases, rate_limited, reset_at = fetch_releases(owner, name)
                    if rate_limited:
                        print("  Still rate limited, skipping to next keyword.")
                        break
                time.sleep(REQUEST_DELAY)

                ipa_assets = extract_ipa_assets(releases)
                if ipa_assets:
                    repos_with_ipa += 1
                    for _ in ipa_assets:
                        print("  IPA found...")
                    new_records = build_dataset(
                        seen_repos, full_name, repo, releases, ipa_assets
                    )
                    all_apps.extend(new_records)
                    print(f"  Apps collected: {len(all_apps)}")

            page += 1
            time.sleep(REQUEST_DELAY)

    save_json(all_apps, OUTPUT_FILE)

    print("\n" + "=" * 50)
    print("FINAL SUMMARY")
    print("=" * 50)
    print(f"Total repositories scanned: {repos_scanned}")
    print(f"Total IPA apps collected:   {len(all_apps)}")
    print("=" * 50)


if __name__ == "__main__":
    main()

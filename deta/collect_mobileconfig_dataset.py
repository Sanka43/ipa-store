"""
Production Mobileconfig Dataset Collector

Discovers .mobileconfig files from public GitHub repositories via the REST API,
downloads them, extracts plist metadata, and builds mobileconfig_dataset.json
until at least 500 valid profiles are collected or search is exhausted.
"""

import json
import os
import re
import time
import xml.etree.ElementTree as ET
from typing import Any, Optional

import requests

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
OUTPUT_DIR = "mobileconfigs"
OUTPUT_JSON = "mobileconfig_dataset.json"
GITHUB_API_BASE = "https://api.github.com"
TARGET_PROFILES = 500
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1.5  # seconds between API calls to avoid rate limits
RATE_LIMIT_RETRY_DELAY = 60
SEARCH_PER_PAGE = 100  # max for repo search

# Search keywords to find repositories that may contain .mobileconfig files
SEARCH_KEYWORDS = [
    "mobileconfig",
    "ios configuration profile",
    "apple configuration profile",
    "mdm profile",
    "ios profile",
]

# Optional: set GITHUB_TOKEN for higher rate limits (5000/hr vs 60/hr)
GITHUB_TOKEN: Optional[str] = None


def get_headers() -> dict:
    """Request headers for GitHub API."""
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Mobileconfig-Dataset-Collector/1.0",
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


# -----------------------------------------------------------------------------
# Plist / XML parsing
# -----------------------------------------------------------------------------
def plist_dict_from_xml_root(root: ET.Element) -> dict[str, Any]:
    """
    Parse plist XML root and return a flat dict of first-level keys to values.
    Extracts string and integer values only.
    """
    result: dict[str, Any] = {}
    dict_elem = root.find(".//dict")
    if dict_elem is None:
        return result
    children = list(dict_elem)
    i = 0
    while i < len(children) - 1:
        key_el = children[i]
        val_el = children[i + 1]
        if key_el.tag != "key":
            i += 1
            continue
        key = (key_el.text or "").strip()
        if val_el.tag == "string":
            text = (val_el.text or "").strip()
            if not text and len(val_el) > 0:
                text = "".join((e.text or "") + (e.tail or "") for e in val_el).strip()
            result[key] = text
        elif val_el.tag == "integer":
            try:
                result[key] = int(val_el.text or "0")
            except ValueError:
                result[key] = val_el.text or ""
        i += 2
    return result


def extract_metadata(content: bytes) -> Optional[dict[str, Any]]:
    """
    Parse .mobileconfig content as plist XML and extract profile metadata.
    Returns None if content is corrupted or invalid XML.
    """
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return None
    plist = plist_dict_from_xml_root(root)
    v = plist.get("PayloadVersion")
    version_str = str(v) if v is not None else ""
    return {
        "name": plist.get("PayloadDisplayName", "") or "",
        "identifier": plist.get("PayloadIdentifier", "") or "",
        "uuid": plist.get("PayloadUUID", "") or "",
        "version": version_str,
        "organization": plist.get("PayloadOrganization", "") or "",
        "description": plist.get("PayloadDescription", "") or "",
    }


def safe_filename(uuid: str, path: str, index: int) -> str:
    """Generate a safe local filename for a .mobileconfig file."""
    if uuid:
        base = re.sub(r"[^\w\-]", "_", uuid)[:80]
    else:
        base = (path.split("/")[-1] or "profile").strip()
        base = re.sub(r"[^\w\-.]", "_", base)
        if not base.lower().endswith(".mobileconfig"):
            base = f"{base}_{index}"
    if not base.lower().endswith(".mobileconfig"):
        base = f"{base}.mobileconfig"
    return base


# -----------------------------------------------------------------------------
# GitHub API
# -----------------------------------------------------------------------------
def search_repositories(keyword: str, page: int = 1) -> tuple[list[dict], bool, Optional[str]]:
    """
    Search GitHub for repositories matching the keyword.
    Returns (list of repo items, rate_limited, reset_at).
    """
    url = f"{GITHUB_API_BASE}/search/repositories"
    params = {
        "q": keyword,
        "sort": "updated",
        "order": "desc",
        "per_page": SEARCH_PER_PAGE,
        "page": page,
    }
    try:
        r = requests.get(url, params=params, headers=get_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code == 403:
            return [], True, r.headers.get("X-RateLimit-Reset")
        r.raise_for_status()
        data = r.json()
        return data.get("items", []), False, None
    except requests.RequestException as e:
        print(f"  [ERROR] Search failed for '{keyword}' page {page}: {e}")
        return [], False, None


def get_repo_default_branch(full_name: str) -> Optional[str]:
    """Get default branch for a repository."""
    url = f"{GITHUB_API_BASE}/repos/{full_name}"
    try:
        r = requests.get(url, headers=get_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None
        return r.json().get("default_branch", "main")
    except requests.RequestException:
        return None


def get_repo_tree_mobileconfig_paths(full_name: str, branch: str) -> list[str]:
    """
    Get recursive tree for repo and return list of paths ending with .mobileconfig.
    """
    url = f"{GITHUB_API_BASE}/repos/{full_name}/git/trees/{branch}"
    params = {"recursive": "1"}
    try:
        r = requests.get(url, params=params, headers=get_headers(), timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return []
        data = r.json()
        tree = data.get("tree", [])
        return [node["path"] for node in tree if node.get("path", "").lower().endswith(".mobileconfig")]
    except (requests.RequestException, KeyError):
        return []


def raw_download_url(full_name: str, branch: str, path: str) -> str:
    """Build raw GitHub content URL for a file."""
    return f"https://raw.githubusercontent.com/{full_name}/{branch}/{path}"


def download_file(url: str) -> Optional[bytes]:
    """Download file from URL; return content or None on error."""
    try:
        r = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        r.raise_for_status()
        return r.content
    except requests.RequestException:
        return None


def wait_for_rate_limit(reset_at: Optional[str]) -> None:
    """Wait until rate limit resets or use default delay."""
    if reset_at:
        try:
            reset_ts = int(reset_at)
            wait = max(10, reset_ts - int(time.time()))
            print(f"  Rate limited. Waiting {wait} seconds...")
            time.sleep(wait)
        except (ValueError, TypeError):
            time.sleep(RATE_LIMIT_RETRY_DELAY)
    else:
        time.sleep(RATE_LIMIT_RETRY_DELAY)


# -----------------------------------------------------------------------------
# Main collection flow
# -----------------------------------------------------------------------------
def collect_repo_full_names() -> list[str]:
    """
    Search GitHub with all keywords and pagination; return unique repo full_name list.
    """
    seen: set[str] = set()
    repo_list: list[str] = []
    for keyword in SEARCH_KEYWORDS:
        page = 1
        while True:
            items, rate_limited, reset_at = search_repositories(keyword, page)
            time.sleep(REQUEST_DELAY)
            if rate_limited:
                wait_for_rate_limit(reset_at)
                continue
            if not items:
                break
            for repo in items:
                full_name = repo.get("full_name", "")
                if full_name and full_name not in seen:
                    seen.add(full_name)
                    repo_list.append(full_name)
            if len(items) < SEARCH_PER_PAGE:
                break
            page += 1
    return repo_list


def save_dataset(profiles: list[dict], path: str) -> None:
    """Write dataset JSON to disk."""
    data = {"profiles": profiles}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def main() -> None:
    global GITHUB_TOKEN
    try:
        GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
    except Exception:
        pass

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    seen_uuids: set[str] = set()
    profiles: list[dict] = []
    repos_scanned = 0
    files_downloaded = 0
    file_index = 0

    # Phase 1: discover repositories
    print("Discovering repositories via GitHub API...")
    repo_list = collect_repo_full_names()
    print(f"  Found {len(repo_list)} unique repositories to scan.\n")

    # Phase 2: scan each repo for .mobileconfig files, download and catalog
    total_repos = len(repo_list)
    for repo_idx, full_name in enumerate(repo_list):
        if len(profiles) >= TARGET_PROFILES:
            print(f"\nTarget of {TARGET_PROFILES} profiles reached.")
            break

        print(f"Scanning repository {repo_idx + 1}/{total_repos}: {full_name}...")
        repos_scanned += 1
        time.sleep(REQUEST_DELAY)

        branch = get_repo_default_branch(full_name)
        if not branch:
            continue
        time.sleep(REQUEST_DELAY)

        paths = get_repo_tree_mobileconfig_paths(full_name, branch)
        if not paths:
            continue

        for path in paths:
            if len(profiles) >= TARGET_PROFILES:
                break
            download_url = raw_download_url(full_name, branch, path)
            time.sleep(REQUEST_DELAY)

            content = download_file(download_url)
            if not content or len(content) < 10:
                continue

            meta = extract_metadata(content)
            if meta is None:
                continue

            uuid_val = (meta.get("uuid") or "").strip()
            if uuid_val and uuid_val in seen_uuids:
                continue
            if uuid_val:
                seen_uuids.add(uuid_val)

            file_index += 1
            filename = safe_filename(uuid_val, path, file_index)
            file_path = os.path.join(OUTPUT_DIR, filename)
            try:
                with open(file_path, "wb") as f:
                    f.write(content)
            except OSError:
                continue

            files_downloaded += 1
            rel_path = os.path.join(OUTPUT_DIR, filename).replace("\\", "/")
            profiles.append({
                "name": meta.get("name", ""),
                "identifier": meta.get("identifier", ""),
                "uuid": meta.get("uuid", ""),
                "version": meta.get("version", ""),
                "organization": meta.get("organization", ""),
                "description": meta.get("description", ""),
                "file_path": rel_path,
                "source_repo": full_name,
                "download_url": download_url,
            })
            print(f"  Profile found: {path} -> {meta.get('name') or '(no name)'}")
            print(f"  Profiles collected: {len(profiles)}")

        # Periodic save
        if profiles:
            save_dataset(profiles, OUTPUT_JSON)

    # Final save
    save_dataset(profiles, OUTPUT_JSON)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Total repositories scanned: {repos_scanned}")
    print(f"  Total profiles collected:   {len(profiles)}")
    print(f"  Total files downloaded:    {files_downloaded}")
    print("=" * 60)
    print(f"\nOutput: {OUTPUT_DIR}/  and  {OUTPUT_JSON}")


if __name__ == "__main__":
    main()

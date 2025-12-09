import os
import sys
import json
import re
import argparse
import requests
from collections import defaultdict

DEFAULT_BASE_URL = "https://cloud.getdbt.com/api/v2"
DEFAULT_MANIFEST_PATH = "target/manifest.json"
DEFAULT_CONFIG_PATH = "job_orchestration_config.json"

TAG_PATTERN = re.compile(r"\btag:([a-zA-Z0-9_\-]+)\b")


def load_manifest_tags(manifest_path: str) -> dict:
    """Return mapping: tag -> set(node_ids) from manifest.json."""
    if not os.path.exists(manifest_path):
        print(
            f"manifest.json not found at '{manifest_path}'.\n"
            "Make sure you are running this from the root of your dbt project "
            "(where dbt_project.yml lives) and that you've run a command like "
            "'dbt compile' or 'dbt build' to generate target/manifest.json.",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    tag_to_nodes = defaultdict(set)

    nodes = manifest.get("nodes", {})
    # Optionally: add sources/exposures if you care about their tags too.

    for node_id, node in nodes.items():
        tags = node.get("tags", []) or []
        for tag in tags:
            tag_to_nodes[tag].add(node_id)

    return tag_to_nodes


def fetch_all_jobs(
    account_id: int, base_url: str, token: str, project_id: int | None
) -> list:
    """Fetch all active dbt Cloud jobs for the given account (and optional project)."""
    headers = {"Authorization": f"Token {token}"}
    jobs = []

    params = {
        "state": 1,  # active only
    }
    if project_id:
        params["project_id"] = project_id

    url = f"{base_url}/accounts/{account_id}/jobs/"

    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        jobs.extend(data.get("data", []))

        # 'next' is a full URL with its own params; after first page, stop passing `params`
        url = data.get("next")
        params = {}

    return jobs


def extract_tags_from_steps(steps) -> set[str]:
    """Find all tag tokens (tag:xyz) in a list of command strings."""
    tags = set()
    for step in steps or []:
        for match in TAG_PATTERN.findall(step):
            tags.add(match)
    return tags


def main():
    parser = argparse.ArgumentParser(
        description="Cross-check dbt model tags (target/manifest.json) against dbt Cloud jobs."
    )
    parser.add_argument(
        "--manifest-path",
        default=DEFAULT_MANIFEST_PATH,
        help=(
            "Path to manifest.json. Default: 'target/manifest.json' "
            "relative to the dbt project root."
        ),
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=(
            "Path to orchestration config JSON (e.g. job_orchestration_config.json). "
            "Used to provide default account_id and base_url."
        ),
    )
    parser.add_argument(
        "--account-id",
        type=int,
        help="dbt Cloud account ID (overrides value from config if provided).",
    )
    parser.add_argument(
        "--base-url",
        help=(
            "dbt Cloud API base URL "
            f"(overrides value from config if provided; default: {DEFAULT_BASE_URL})."
        ),
    )
    parser.add_argument(
        "--project-id",
        type=int,
        help="Optional dbt Cloud project_id filter for jobs.",
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help="Print warnings but always exit with code 0 (non-strict mode).",
    )
    args = parser.parse_args()

    token = os.getenv("DBT_API_TOKEN")
    if not token:
        print("DBT_API_TOKEN environment variable is not set", file=sys.stderr)
        sys.exit(1)

    # Load defaults from config, if present
    cfg = {}
    if os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        print(
            f"Config file '{args.config}' not found. Continuing with CLI/default values.",
            file=sys.stderr,
        )

    # Resolve account_id and base_url precedence: CLI > config > default
    account_id = args.account_id or cfg.get("account_id")
    base_url = args.base_url or cfg.get("base_url") or DEFAULT_BASE_URL

    if not account_id:
        print(
            "account_id is not set. Provide it via --account-id or in the config JSON.",
            file=sys.stderr,
        )
        sys.exit(1)

    project_id = args.project_id  # optional filter

    # 1) Tags from manifest
    manifest_path = args.manifest_path
    print(f"Loading manifest from: {manifest_path}")
    tag_to_nodes = load_manifest_tags(manifest_path)
    manifest_tags = set(tag_to_nodes.keys())
    print(f"Found {len(manifest_tags)} unique tags in manifest.json")

    # 2) Jobs from dbt Cloud
    print(
        f"Fetching jobs for account_id={account_id}"
        + (f", project_id={project_id}" if project_id else "")
        + f" from {base_url}"
    )
    jobs = fetch_all_jobs(account_id, base_url, token, project_id)
    print(f"Found {len(jobs)} active jobs in dbt Cloud\n")

    # 3) Tags inferred from job commands + job name convention check
    tag_to_jobs = defaultdict(set)
    jobs_using_tags_without_name = []  # (job_id, job_name, tags_missing_in_name)

    for job in jobs:
        job_id = job["id"]
        job_name = job["name"]

        steps = (
            job.get("execute_steps")
            or job.get("settings", {}).get("execute_steps", [])
        )

        used_tags = extract_tags_from_steps(steps)
        if not used_tags:
            continue

        for tag in used_tags:
            tag_to_jobs[tag].add(job_id)

        # Naming convention: job name should contain 'tag:<tag>'
        missing_in_name = [
            tag for tag in used_tags if f"tag:{tag}".lower() not in job_name.lower()
        ]
        if missing_in_name:
            jobs_using_tags_without_name.append((job_id, job_name, missing_in_name))

    job_tags = set(tag_to_jobs.keys())
    print(f"Found {len(job_tags)} unique tags referenced in job commands\n")

    # 4) Compute diagnostics
    tags_in_manifest_without_jobs = sorted(manifest_tags - job_tags)
    tags_in_jobs_not_in_manifest = sorted(job_tags - manifest_tags)
    tags_with_models_and_jobs = sorted(manifest_tags & job_tags)

    # 5) Report

    # A) Tags that have both models and jobs (this is the happy path)
    print("=== Tags with matching models AND jobs ===")
    if tags_with_models_and_jobs:
        for tag in tags_with_models_and_jobs:
            node_count = len(tag_to_nodes[tag])
            job_count = len(tag_to_jobs[tag])
            print(f"  - {tag} (models: {node_count}, jobs: {job_count})")
    else:
        print("  (none)")
    print("")

    # B) Tags defined on models but never used by any job
    print("=== Tags in manifest with NO corresponding jobs ===")
    if tags_in_manifest_without_jobs:
        for tag in tags_in_manifest_without_jobs:
            print(f"  - {tag} (used on {len(tag_to_nodes[tag])} nodes)")
    else:
        print("  (none)")
    print("")

    # C) Tags used in jobs but not present in manifest
    print("=== Tags referenced in job commands but NOT found in manifest ===")
    if tags_in_jobs_not_in_manifest:
        for tag in tags_in_jobs_not_in_manifest:
            job_count = len(tag_to_jobs[tag])
            print(f"  - {tag} (used by {job_count} jobs)")
    else:
        print("  (none)")
    print("")

    # D) Jobs that use tags in commands but don't follow the naming convention
    print(
        "=== Jobs that use tags in commands but do NOT include 'tag:<tag>' in the job name ==="
    )
    if jobs_using_tags_without_name:
        for job_id, job_name, missing_tags in jobs_using_tags_without_name:
            tags_str = ", ".join(missing_tags)
            print(f"  - {job_id}: {job_name} (missing: {tags_str})")
    else:
        print("  (none)")
    print("")

    # 6) Exit behavior (strict vs warn-only)
    issues_found = (
        tags_in_manifest_without_jobs
        or tags_in_jobs_not_in_manifest
        or jobs_using_tags_without_name
    )

    if issues_found:
        if args.warn_only:
            print("\n⚠️  Warnings mismatch in tags and jobs found, but config set to --warn-only. Exiting with code 0.\n")
            sys.exit(0)
        else:
            print("\n❌ Mismatch in tags and jobs found. Exiting with code 1.\n")
            sys.exit(1)

    print("✅ Tag usage between manifest and jobs looks consistent.")
    sys.exit(0)


if __name__ == "__main__":
    main()
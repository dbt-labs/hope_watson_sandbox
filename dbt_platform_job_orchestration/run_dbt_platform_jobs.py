import argparse
import json
import os
import time
import requests

SUCCESS = {10}
FAILURE = {20, 30}


def list_jobs(account_id, base_url, token, env_id=None, name_filter=None):
    """List dbt Cloud jobs, optionally filtered by environment_id and name__icontains."""
    jobs = []
    params = {
        "state": 1,  # active only
        "limit": 100,
    }
    if env_id:
        params["environment_id"] = env_id
    if name_filter:
        params["name__icontains"] = name_filter

    url = f"{base_url}/accounts/{account_id}/jobs/"
    headers = {"Authorization": f"Token {token}"}

    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        jobs.extend(data.get("data", []))
        url = data.get("next")
        # after first page, params are baked into `next` URL
        params = {}

    return jobs


def trigger_job(account_id, base_url, token, job_id):
    """Trigger a dbt Cloud job and return run_id."""
    url = f"{base_url}/accounts/{account_id}/jobs/{job_id}/run/"
    headers = {"Authorization": f"Token {token}"}
    payload = {
        "cause": "Triggered via run_dbt_platform_jobs script"
    }
    resp = requests.post(url, headers=headers, json=payload)

    if not resp.ok:
        print(f"\nError triggering job {job_id}: {resp.status_code}")
        try:
            print("Response JSON:", resp.json())
        except Exception:
            print("Response text:", resp.text)
        resp.raise_for_status()

    return resp.json()["data"]["id"]  # run_id



def run_status(account_id, base_url, token, run_id):
    """Get numeric + humanized status for a run."""
    url = f"{base_url}/accounts/{account_id}/runs/{run_id}/"
    headers = {"Authorization": f"Token {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()["data"]
    return data["status"], data["status_humanized"]


def main():
    parser = argparse.ArgumentParser(description="Minimal dbt job orchestrator")
    parser.add_argument("--config", required=True, help="Path to config.json")
    parser.add_argument(
        "--env",
        required=False,
        help="Environment key from config (e.g. prod, qa). Optional; if omitted, no environment filter is used.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not trigger jobs, just list them",
    )
    parser.add_argument(
        "--name-icontains",
        dest="name_icontains",
        help="Use job API name__icontains filter (case-insensitive substring on job name)",
    )
    parser.add_argument(
    "--no-wait",
    action="store_true",
    help="Trigger jobs but do not wait for completion.",
    )

    args = parser.parse_args()

    token = os.getenv("DBT_API_TOKEN")
    if not token:
        raise SystemExit("DBT_API_TOKEN environment variable not set")

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    account_id = cfg["account_id"]
    base_url = cfg["base_url"]
    env_map = cfg.get("environments", {})
    poll = cfg.get("poll_interval_seconds", 15)

    # Resolve env_id (optional)
    env_id = None
    if args.env:
        if args.env not in env_map:
            raise SystemExit(
                f"Environment '{args.env}' not found in config. Available: {', '.join(env_map.keys())}"
            )
        env_id = env_map[args.env]

    # Logging what we're about to do
    if env_id:
        print(f"Listing jobs in environment '{args.env}' (id={env_id})...")
    else:
        print("Listing jobs across the entire account (no environment filter)...")

    if args.name_icontains:
        print(f"Using name__icontains filter: '{args.name_icontains}'")

    jobs = list_jobs(account_id, base_url, token, env_id, args.name_icontains)
    print(f"Found {len(jobs)} jobs matching filters.")

    # Always print the jobs returned
    print("\nJobs returned by API based on filters:")
    for j in jobs:
        print(f"  - {j['id']}: {j['name']}")
    print("")

    if not jobs:
        print("No jobs matched. Exiting.")
        return

    if args.dry_run:
        print("\n--- DRY RUN ---")
        print("The following jobs *would* be triggered:")
        for j in jobs:
            print(f"  - {j['id']}: {j['name']}")
        print("\nNo calls were made to dbt Cloud.")
        return

    print(f"\nTriggering {len(jobs)} jobs...")
    run_ids = {}
    for j in jobs:
        run_id = trigger_job(account_id, base_url, token, j["id"])
        print(f"Triggered job {j['id']} ({j['name']}) -> run_id={run_id}")
        run_ids[run_id] = j["name"]
    if args.no_wait:
        print("\n--no-wait flag set: not polling for job completion.\n")
        print("Triggered job run_ids:")
        for rid, name in run_ids.items():
            print(f"  - {rid}: {name}")
        return  # exit here cleanly

    print("\nPolling until all runs finish...\n")
    running = set(run_ids.keys())

    while running:
        time.sleep(poll)
        for rid in list(running):
            status, status_h = run_status(account_id, base_url, token, rid)
            if status in SUCCESS:
                print(f"SUCCESS: run {rid} ({run_ids[rid]})")
                running.remove(rid)
            elif status in FAILURE:
                print(f"FAILURE: run {rid} ({run_ids[rid]}) — {status_h}")
                running.remove(rid)
            else:
                print(f"RUNNING: run {rid} ({run_ids[rid]}) — {status_h}")
        print(f"\n{len(running)} still running...\n")

    print("All jobs completed.")


if __name__ == "__main__":
    main()

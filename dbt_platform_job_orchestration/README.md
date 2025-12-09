# dbt Platform Job Orchestration

This folder contains lightweight tooling for orchestrating **dbt platform jobs** using the dbt Platform API. It supports running job groups, filtering by naming patterns, and triggering jobs either synchronously or fire-and-forget. Configuration stays simple, and schedulers like Control-M can call the script directly.

Additionally, there is an audit script to ensure model tags and job names match. 

---

## Files

### `run_dbt_platform_jobs.py`
A Python script that:
- Lists dbt Platform jobs (optionally filtered by environment or name)
- Supports:
  - `--dry-run` (list jobs only)
  - `--no-wait` (trigger and exit immediately)
  - default mode (trigger + poll for completion)
- Uses `name__icontains` to group jobs by naming conventions (e.g., `tag:suppliers`)
- Prints job IDs, run IDs, and results

### `job_orchestration_config.json`
Holds stable configuration:
- `account_id`
- `base_url`
- optional `environments` mapping
- `poll_interval_seconds`

### `audit_tags_vs_jobs.py` (Tag Audit Script)

This script cross-checks dbt model tags from `target/manifest.json` against dbt Platform jobs.

It verifies:
1. Tags defined on models **that also have matching jobs**
2. Tags in the manifest **with no corresponding jobs**
3. Tags referenced in job commands **that don’t exist in the manifest**
4. Jobs using tags in commands **but missing the naming token `tag:<tag>` in the job name**

This helps ensure consistency between:
- dbt model tagging (in the project)
- dbt job grouping (via naming conventions)

Run from the **dbt project root**:

For example to run the audit on a specific project and to warn only the mismatches:
```bash
python dbt_platform_job_orchestration/audit_tags_vs_jobs.py \
  --config dbt_platform_job_orchestration/job_orchestration_config.json \
  --project-id 381975 \
  --warn-only
```
This will output your results of the 4 categories to help identify if model tags and job names do match otherwise list out how they mismatch. 

Note: Job grouping is intentionally passed in at runtime using CLI flags, not stored in config.

---

## Orchestration Usage

Set your dbt Platform API token:

```bash
export DBT_API_TOKEN="your_token_here"
``` 
I set my token in my `.zshrc` for local macOS development. It should be set to the equivalent on windows and use a secrets manager for control-M. 

## Example job trigger commands

### Dry run all jobs in prod:
```bash 
python run_dbt_platform_jobs.py \
    --config job_orchestration_config.json \
    --env prod \
    --dry-run 
```
### Run jobs matching a naming token (e.g., `tag:suppliers`) with default polling:

```bash
python run_dbt_platform_jobs.py \
    --config job_orchestration_config.json \
    --name-icontains "tag:suppliers" 
```

### Trigger without waiting for completion (no wait)
```bash
python run_dbt_platform_jobs.py \
    --config job_orchestration_config.json \
    --name-icontains "tag:suppliers" \
    --no-wait
```
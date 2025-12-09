# dbt Platform Job Orchestration

This folder contains lightweight tooling for orchestrating **dbt platform jobs** using the dbt Platform API. It supports running job groups, filtering by naming patterns, and triggering jobs either synchronously or fire-and-forget. Configuration stays simple, and schedulers like Control-M can call the script directly.

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

Job grouping is intentionally passed in at runtime using CLI flags, not stored in config.

---

## Usage

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
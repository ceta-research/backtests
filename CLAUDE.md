# backtests (ceta-research/backtests)

```yaml
brand_strategy:
  primary: "Ceta Research"
  secondary: "Trading Studio"
  note: "CR is the primary brand for API, compute, and content. TS stays for finance-specific blog/SEO. Blog content double-posts to both brands."
  backward_compat: "ts_client.py re-exports CetaResearch as TradingStudio. TS_API_KEY falls back if CR_API_KEY not set."

api_surfaces:
  data_explorer:
    client: "cr_client.py → CetaResearch.query()"
    base_url: "https://api.cetaresearch.com/api/v1"
    env_var: "CR_API_KEY (falls back to TS_API_KEY)"
    formats: ["json", "csv", "parquet"]
  code_execution:
    client: "cr_client.py → CetaResearch.execute_code()"
    endpoints: ["POST /code-executions", "GET /code-executions/{taskId}", "DELETE /code-executions/{taskId}", "GET /code-executions/limits"]
    csrf: "Required for POST/DELETE. Client auto-fetches via GET /auth/csrf-token"
  projects:
    client: "cr_client.py → CetaResearch.create_project(), .upsert_file(), .run_project()"
    endpoints: ["POST /projects", "PUT /projects/{id}/files", "POST /projects/{id}/run", "GET /projects/{id}/runs/{runId}"]
    csrf: "Required for POST/PUT/DELETE"

file_structure:
  shared: ["cr_client.py", "ts_client.py (shim)", "data_utils.py", "metrics.py", "costs.py", "cli_utils.py", "cloud_runner.py"]
  strategies: ["qarp/", "piotroski/", "low-pe/"]
  examples: ["examples/code_execution_example.py", "examples/projects_example.py"]

cli_conventions:
  exchange: "--exchange BSE,NSE or --preset india or --global"
  cloud: "--cloud flag on all screen/backtest scripts"
  api_key: "--api-key flag, falls back to CR_API_KEY then TS_API_KEY env var"
  output: "--output results.json --verbose"

execution_modes:
  local: "Default. Fetches data via API, caches in DuckDB, runs locally."
  cloud_code_exec: "--cloud on screen scripts. Uses Code Execution API."
  cloud_projects: "--cloud on backtest scripts. Uses Projects API via cloud_runner.py."

known_risks:
  - "Code Execution / Projects APIs may have response shape differences. Fix as discovered."
  - "CSRF required for POST endpoints even with API key auth."
  - "Cloud execution may not have CR_API_KEY in environment. cloud_runner.py handles this."
```

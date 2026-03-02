"""Cloud execution helper for multi-file backtests via the Projects API.

Handles project creation, file upload, and execution for strategies that
need multiple shared modules (cr_client.py, data_utils.py, metrics.py, etc.)

Usage:
    from cloud_runner import run_backtest_cloud

    result = run_backtest_cloud(
        strategy="qarp",
        args_str="--preset us --verbose",
        api_key="your_key",
    )
    print(result["stdout"])
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cr_client import CetaResearch, ExecutionError

# Shared files that every strategy needs
SHARED_FILES = [
    "cr_client.py",
    "data_utils.py",
    "metrics.py",
    "costs.py",
    "cli_utils.py",
]

# Strategy-specific files
STRATEGY_FILES = {
    "qarp": ["qarp/backtest.py", "qarp/screen.py"],
    "piotroski": ["piotroski/backtest.py", "piotroski/screen.py"],
    "low-pe": ["low-pe/backtest.py", "low-pe/screen.py"],
}

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


def _read_file(path):
    """Read a file relative to the repo root."""
    full_path = os.path.join(REPO_ROOT, path)
    with open(full_path, "r") as f:
        return f.read()


def _find_or_create_project(cr, strategy):
    """Find existing project or create a new one for this strategy."""
    project_name = f"ts-backtests-{strategy}"

    # Check if project already exists
    projects = cr.list_projects(limit=100)
    for p in projects.get("projects", []):
        if p["name"] == project_name:
            return p

    # Create new project
    return cr.create_project(
        name=project_name,
        language="python",
        entrypoint=f"{strategy}/backtest.py",
        dependencies=["requests", "duckdb"],
        description=f"ts-backtests {strategy} strategy (auto-managed by cloud_runner)",
    )


def _upload_files(cr, project_id, file_paths, verbose=False):
    """Upload a list of files to the project."""
    for file_path in file_paths:
        content = _read_file(file_path)
        if verbose:
            print(f"  Uploading {file_path}...")
        cr.upsert_file(project_id, file_path, content)

    if verbose:
        print(f"  Uploaded {len(file_paths)} files")


def _make_wrapper(entry_script, args_list, api_key):
    """Build a wrapper script that sets sys.argv, env, and runs the entry script."""
    return f"""import sys, os
os.environ["CR_API_KEY"] = {api_key!r}
sys.path.insert(0, os.getcwd())
__file__ = {entry_script!r}
sys.argv = [{os.path.basename(entry_script)!r}] + {args_list!r}
exec(open({entry_script!r}).read())
"""


def run_backtest_cloud(strategy, args_str="", api_key=None, base_url=None,
                       cpu_count=2, ram_mb=4096, timeout_seconds=600,
                       verbose=False):
    """Run a backtest on the cloud via the Projects API.

    Args:
        strategy: Strategy name ("qarp", "piotroski", "low-pe").
        args_str: CLI arguments as a string (e.g. "--preset us --verbose").
        api_key: API key (falls back to CR_API_KEY env var).
        base_url: API base URL.
        cpu_count: CPU cores for execution.
        ram_mb: RAM in MB.
        timeout_seconds: Max execution time.
        verbose: Print progress.

    Returns:
        dict with run result (stdout, stderr, status, etc.)
    """
    cr = CetaResearch(api_key=api_key, base_url=base_url)

    # Find or create project
    if verbose:
        print(f"Setting up cloud project for {strategy}...")
    project = _find_or_create_project(cr, strategy)
    project_id = project["id"]
    if verbose:
        print(f"  Project: {project['name']} ({project_id})")

    # Upload shared + strategy files
    files = SHARED_FILES + STRATEGY_FILES.get(strategy, [])
    _upload_files(cr, project_id, files, verbose=verbose)

    # Create entry wrapper that sets sys.argv and API key
    entry_script = f"{strategy}/backtest.py"
    args_list = args_str.split() if args_str else []
    wrapper = _make_wrapper(entry_script, args_list, cr.api_key)
    cr.upsert_file(project_id, "_run.py", wrapper)

    # Run project
    if verbose:
        print(f"  Submitting run...")
    result = cr.run_project(
        project_id,
        entry_path="_run.py",
        cpu_count=cpu_count,
        ram_mb=ram_mb,
        timeout_seconds=timeout_seconds,
        verbose=verbose,
    )

    return result


def run_screen_cloud(strategy, args_str="", api_key=None, base_url=None,
                     timeout_seconds=120, verbose=False):
    """Run a screen script on the cloud via the Projects API.

    Screens need cr_client.py and cli_utils.py, so we use the Projects API
    to upload them as a mini project.

    Args:
        strategy: Strategy name ("qarp", "piotroski", "low-pe").
        args_str: CLI arguments as a string.
        api_key: API key.
        base_url: API base URL.
        timeout_seconds: Max execution time.
        verbose: Print progress.

    Returns:
        dict with execution result (stdout, stderr, status, etc.)
    """
    cr = CetaResearch(api_key=api_key, base_url=base_url)

    # Find or create project (reuses same project as backtest)
    if verbose:
        print(f"Setting up cloud project for {strategy}...")
    project = _find_or_create_project(cr, strategy)
    project_id = project["id"]

    # Upload minimal files for screen
    screen_path = f"{strategy}/screen.py"
    files = ["cr_client.py", "cli_utils.py", screen_path]
    _upload_files(cr, project_id, files, verbose=verbose)

    # Create entry wrapper with API key embedded
    args_list = args_str.split() if args_str else []
    wrapper = _make_wrapper(screen_path, args_list, cr.api_key)
    cr.upsert_file(project_id, "_run_screen.py", wrapper)

    if verbose:
        print(f"  Submitting run...")
    result = cr.run_project(
        project_id,
        entry_path="_run_screen.py",
        cpu_count=1,
        ram_mb=512,
        timeout_seconds=timeout_seconds,
        verbose=verbose,
    )

    return result

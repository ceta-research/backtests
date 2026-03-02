#!/usr/bin/env python3
"""Example: Create a project and run a backtest via the Projects API.

This demonstrates the full Projects API workflow:
  1. Create a project
  2. Upload multiple files
  3. Run the project on cloud compute
  4. Poll for results

Usage:
    export CR_API_KEY="your_key"
    python3 examples/projects_example.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cr_client import CetaResearch

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def read_file(path):
    with open(os.path.join(REPO_ROOT, path), "r") as f:
        return f.read()


def main():
    cr = CetaResearch()

    # 1. Create project
    print("Creating project...")
    project = cr.create_project(
        name="qarp-backtest-example",
        language="python",
        entrypoint="run.py",
        dependencies=["requests", "duckdb"],
        description="QARP backtest example via Projects API",
    )
    project_id = project["id"]
    print(f"  Project created: {project['name']} ({project_id})")

    # 2. Upload files
    files = [
        "cr_client.py",
        "data_utils.py",
        "metrics.py",
        "costs.py",
        "cli_utils.py",
        "qarp/backtest.py",
    ]

    print("Uploading files...")
    for f in files:
        content = read_file(f)
        cr.upsert_file(project_id, f, content)
        print(f"  Uploaded: {f}")

    # 3. Create entry script
    run_script = """import sys
sys.argv = ["backtest.py", "--preset", "us"]

import os
sys.path.insert(0, os.getcwd())
exec(open("qarp/backtest.py").read())
"""
    cr.upsert_file(project_id, "run.py", run_script)
    print("  Uploaded: run.py (entry point)")

    # 4. Run on cloud
    print("\nRunning backtest on cloud...")
    result = cr.run_project(
        project_id,
        entry_path="run.py",
        cpu_count=2,
        ram_mb=4096,
        timeout_seconds=600,
        verbose=True,
    )

    # 5. Print results
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    if result.get("stdout"):
        print(result["stdout"])
    if result.get("stderr"):
        print("\nSTDERR:", result["stderr"])

    status = result.get("status", "unknown")
    print(f"\nStatus: {status}")
    if result.get("executionTimeMs"):
        print(f"Execution time: {result['executionTimeMs'] / 1000:.1f}s")

    # 6. Check for output files
    run_id = result.get("id") or result.get("taskId")
    if run_id and status == "completed":
        try:
            files = cr.get_run_files(project_id, run_id)
            if files:
                print(f"\nOutput files: {len(files)}")
                for f in files:
                    print(f"  {f.get('name', f.get('path', '?'))}")
        except Exception:
            pass

    # Cleanup: list projects to confirm
    projects = cr.list_projects()
    print(f"\nYou now have {projects.get('totalCount', '?')} projects")
    print(f"Delete with: cr.delete_project('{project_id}')")


if __name__ == "__main__":
    main()

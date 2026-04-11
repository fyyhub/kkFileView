#!/usr/bin/env python3
import base64
import os
import pathlib
import sys
import uuid

import winrm


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value if value else default


def ps_quote(value: str) -> str:
    return value.replace("'", "''")


def main() -> int:
    host = require_env("KK_DEPLOY_HOST")
    port = optional_env("KK_DEPLOY_PORT", "5985")
    username = require_env("KK_DEPLOY_USERNAME")
    password = require_env("KK_DEPLOY_PASSWORD")
    deploy_root = optional_env("KK_DEPLOY_ROOT", r"C:\kkFileView-5.0")
    health_url = optional_env("KK_DEPLOY_HEALTH_URL", "http://127.0.0.1:8012/")
    artifact_name = optional_env("KK_DEPLOY_ARTIFACT_NAME", "kkfileview-server-jar")
    repository = require_env("GITHUB_REPOSITORY_NAME")
    run_id = require_env("GITHUB_RUN_ID_VALUE")
    artifact_token = require_env("KK_DEPLOY_ARTIFACT_TOKEN")
    dry_run = optional_env("KK_DEPLOY_DRY_RUN", "false").lower()

    script_path = pathlib.Path(__file__).with_name("remote_windows_deploy.ps1")
    script_body = script_path.read_text(encoding="utf-8")
    payload = script_body.encode("utf-8-sig")
    payload_b64 = base64.b64encode(payload).decode("ascii")

    endpoint = f"http://{host}:{port}/wsman"
    session = winrm.Session(endpoint, auth=(username, password), transport="ntlm")

    suffix = uuid.uuid4().hex
    remote_b64_path = fr"C:\Windows\Temp\kkfileview_deploy_{suffix}.b64"
    remote_ps1_path = fr"C:\Windows\Temp\kkfileview_deploy_{suffix}.ps1"

    prep = session.run_ps(
        f"""
$ErrorActionPreference = 'Stop'
if (Test-Path '{ps_quote(remote_b64_path)}') {{ Remove-Item '{ps_quote(remote_b64_path)}' -Force }}
if (Test-Path '{ps_quote(remote_ps1_path)}') {{ Remove-Item '{ps_quote(remote_ps1_path)}' -Force }}
New-Item -ItemType File -Path '{ps_quote(remote_b64_path)}' -Force | Out-Null
"""
    )
    if prep.status_code != 0:
        sys.stderr.write(prep.std_err.decode("utf-8", errors="ignore"))
        return prep.status_code

    chunk_size = 1200
    for start in range(0, len(payload_b64), chunk_size):
        chunk = payload_b64[start : start + chunk_size]
        append = session.run_ps(
            f"Add-Content -LiteralPath '{ps_quote(remote_b64_path)}' -Value '{chunk}'"
        )
        if append.status_code != 0:
            sys.stderr.write(append.std_err.decode("utf-8", errors="ignore"))
            return append.status_code

    result = session.run_ps(
        f"""
$ErrorActionPreference = 'Stop'
$raw = Get-Content -LiteralPath '{ps_quote(remote_b64_path)}' -Raw
[System.IO.File]::WriteAllBytes('{ps_quote(remote_ps1_path)}', [Convert]::FromBase64String($raw))
try {{
  powershell -NoProfile -ExecutionPolicy Bypass -File '{ps_quote(remote_ps1_path)}' `
    -Repository '{ps_quote(repository)}' `
    -RunId '{ps_quote(run_id)}' `
    -ArtifactName '{ps_quote(artifact_name)}' `
    -GitHubToken '{ps_quote(artifact_token)}' `
    -DeployRoot '{ps_quote(deploy_root)}' `
    -HealthUrl '{ps_quote(health_url)}' `
    -DryRun '{ps_quote(dry_run)}'
  $code = $LASTEXITCODE
}} finally {{
  Remove-Item '{ps_quote(remote_b64_path)}' -Force -ErrorAction SilentlyContinue
  Remove-Item '{ps_quote(remote_ps1_path)}' -Force -ErrorAction SilentlyContinue
}}
exit $code
"""
    )

    stdout = result.std_out.decode("utf-8", errors="ignore").strip()
    stderr = result.std_err.decode("utf-8", errors="ignore").strip()

    if stdout:
        print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)

    return result.status_code


if __name__ == "__main__":
    raise SystemExit(main())

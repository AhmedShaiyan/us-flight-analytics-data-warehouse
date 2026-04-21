

from __future__ import annotations
import os


def load_secrets() -> None:
    if not _is_cloud_run():
        return

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        return

    _fetch_secret(project_id, "ANTHROPIC_API_KEY")


def _is_cloud_run() -> bool:
    return os.environ.get("K_SERVICE") is not None


def _fetch_secret(project_id: str, secret_name: str) -> None:
    if os.environ.get(secret_name):
        return  # already injected via --set-secrets

    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        os.environ[secret_name] = response.payload.data.decode("utf-8")
    except Exception as exc:
        print(f"[secrets] WARNING: Could not load {secret_name} from Secret Manager: {exc}")

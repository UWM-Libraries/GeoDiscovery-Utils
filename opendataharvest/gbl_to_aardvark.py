import json
import logging
import os
from pathlib import Path
import subprocess
import sys

import yaml

CONFIG_DIR = Path(__file__).resolve().parent

# Load configuration from YAML file
with open(CONFIG_DIR / "config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Get the OGM_PATH environment variable or from config
env_ogm_path = os.getenv("OGM_PATH")
if os.getenv("RAILS_ENV") == "production" and not env_ogm_path:
    raise ValueError("OGM_PATH must be set in production")
ogm_path = env_ogm_path or str((CONFIG_DIR / config["paths"]["ogm_path"]).resolve())

# Set up logging to a file
logfile = config["logging"]["logfile"]
if not os.path.isabs(logfile):
    logfile = str((CONFIG_DIR / logfile).resolve())
level = getattr(logging, config["logging"]["level"].upper(), logging.ERROR)
logging.basicConfig(
    filename=logfile,
    level=level,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

REPOS = [
    # {"name": "edu.berkeley"},
    {"name": "edu.princeton.arks"},
    {"name": "edu.stanford.purl"},
    {"name": "edu.cornell"},
    {"name": "edu.columbia"},
    {"name": "edu.wisc", "extra_args": ["--place_default", "Wisconsin"]},
]


def iter_json_records(root: Path):
    for path in root.rglob("*.json"):
        if path.name == "layers.json":
            continue

        try:
            with open(path, encoding="utf8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError):
            continue

        if isinstance(payload, dict):
            yield path, payload
        elif isinstance(payload, list):
            for record in payload:
                if isinstance(record, dict):
                    yield path, record


def has_aardvark_metadata(repo_path: Path) -> bool:
    aardvark_dirs = [repo_path / "metadata-aardvark", repo_path / "aardvark"]
    for aardvark_dir in aardvark_dirs:
        if not aardvark_dir.is_dir():
            continue
        for _path, record in iter_json_records(aardvark_dir):
            version = record.get("gbl_mdVersion_s") or record.get(
                "geoblacklight_version"
            )
            if version == "Aardvark":
                return True
    return False


def legacy_source_dir(repo_path: Path) -> Path:
    metadata_1 = repo_path / "metadata-1.0"
    return metadata_1 if metadata_1.is_dir() else repo_path


def has_legacy_metadata(repo_path: Path) -> bool:
    for _path, record in iter_json_records(repo_path):
        if record.get("geoblacklight_version") == "1.0":
            return True
    return False


def main() -> None:
    python_bin = Path(sys.executable)
    convert_script = CONFIG_DIR / "convert.py"

    for repo in REPOS:
        repo_path = Path(ogm_path) / repo["name"]

        if not repo_path.is_dir():
            logging.info(
                f"Skipping conversion for {repo['name']}: local repository path does not exist."
            )
            continue

        if has_aardvark_metadata(repo_path):
            logging.info(
                f"Skipping conversion for {repo['name']}: populated Aardvark metadata already exists."
            )
            continue

        source_dir = legacy_source_dir(repo_path)
        if not has_legacy_metadata(source_dir):
            logging.warning(
                f"Skipping conversion for {repo['name']}: no legacy GeoBlacklight 1.0 JSON files were found."
            )
            continue

        target_dir = repo_path / "aardvark"
        command = [
            str(python_bin),
            str(convert_script),
            str(source_dir),
            str(target_dir),
        ] + repo.get("extra_args", [])

        try:
            subprocess.run(command, check=True)
            logging.info(f"Command {' '.join(command)} executed successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(
                f"Command {' '.join(command)} failed with return code {e.returncode}."
            )
            if e.stderr:
                logging.error(f"Error message: {e.stderr}")


if __name__ == "__main__":
    main()

import os
import subprocess
import logging
import yaml
from pathlib import Path

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

commands = [
    # [
    #     "lib/opendataharvest/venv/bin/python3",
    #     "lib/opendataharvest/src/opendataharvest/convert.py",
    #     f"{ogm_path}/edu.berkeley/",
    #     f"{ogm_path}/edu.berkeley/aardvark",
    # ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/src/opendataharvest/convert.py",
        f"{ogm_path}/edu.princeton.arks/",
        f"{ogm_path}/edu.princeton.arks/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/src/opendataharvest/convert.py",
        f"{ogm_path}/edu.stanford.purl/",
        f"{ogm_path}/edu.stanford.purl/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/src/opendataharvest/convert.py",
        f"{ogm_path}/edu.cornell/",
        f"{ogm_path}/edu.cornell/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/src/opendataharvest/convert.py",
        f"{ogm_path}/edu.columbia/",
        f"{ogm_path}/edu.columbia/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/src/opendataharvest/convert.py",
        f"{ogm_path}/edu.wisc/",
        f"{ogm_path}/edu.wisc/aardvark",
        "--place_default",
        "Wisconsin",
    ],
]

for command in commands:
    try:
        result = subprocess.run(command, check=True) #, capture_output=True, text=True) (This fails in production)
        logging.info(f"Command {' '.join(command)} executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Command {' '.join(command)} failed with return code {e.returncode}."
        )
        logging.error(f"Error message: {e.stderr}")

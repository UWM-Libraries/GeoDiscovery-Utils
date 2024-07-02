import os
import subprocess
import logging

# Get the OGM_PATH environment variable
ogm_path = os.getenv(
    "OGM_PATH", "/var/www/rubyapps/uwm-geoblacklight/shared/tmp/opengeometadata"
)  # Default to '/var/www/rubyapps/uwm-geoblacklight/shared/tmp/opengeometadata' if not set

# Set up logging to a file
logging.basicConfig(
    filename="log/gbl_to_aardvark.log",
    level=logging.ERROR,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

commands = [
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/gbl-1_to_aardvark/convert.py",
        f"{ogm_path}/edu.berkeley/",
        f"{ogm_path}/edu.berkeley/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/gbl-1_to_aardvark/convert.py",
        f"{ogm_path}/edu.princeton.arks/",
        f"{ogm_path}/edu.princeton.arks/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/gbl-1_to_aardvark/convert.py",
        f"{ogm_path}/edu.stanford.purl/",
        f"{ogm_path}/edu.stanford.purl/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/gbl-1_to_aardvark/convert.py",
        f"{ogm_path}/edu.cornell/",
        f"{ogm_path}/edu.cornell/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/gbl-1_to_aardvark/convert.py",
        f"{ogm_path}/edu.columbia/",
        f"{ogm_path}/edu.columbia/aardvark",
    ],
    [
        "lib/opendataharvest/venv/bin/python3",
        "lib/opendataharvest/gbl-1_to_aardvark/convert.py",
        f"{ogm_path}/edu.wisc/",
        f"{ogm_path}/edu.wisc/aardvark",
        "--place_default",
        "Wisconsin",
    ],
]

for command in commands:
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(f"Command {' '.join(command)} executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(
            f"Command {' '.join(command)} failed with return code {e.returncode}."
        )
        logging.error(f"Error message: {e.stderr}")

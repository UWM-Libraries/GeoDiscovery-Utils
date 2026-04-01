import argparse
import json
import logging
import os
from pathlib import Path
import subprocess
from typing import Dict, Iterable, Optional
import tempfile
import unicodedata

import yaml
from classify import ResourceClassifier

CONFIG_DIR = Path(__file__).resolve().parent

# Load configuration from YAML file
with open(CONFIG_DIR / "config.yaml", "r") as file:
    config = yaml.safe_load(file)


class StanfordSpatialNormalizer:
    @staticmethod
    def normalize(data_dict: Dict) -> bool:
        """Collapse a known bad Stanford place combination to a broader place."""
        spatial = data_dict.get("dct_spatial_sm") or []
        if "Wisconsin" in spatial and "New Mexico" in spatial:
            data_dict["dct_spatial_sm"] = ["United States"]
            return True
        return False


class OpenIndexMapsNormalizer:
    @staticmethod
    def normalize(data_dict: Dict) -> bool:
        """Restore OpenIndexMaps class/type logic for harvested Aardvark records."""

        def with_unique_items(items, additions):
            result = list(items)
            for item in additions:
                if item not in result:
                    result.append(item)
            return result

        dct_references_s = str(data_dict.get("dct_references_s", ""))
        identifier = str(data_dict.get("id", ""))
        dct_source_sm = str(data_dict.get("dct_source_sm", ""))
        dct_description_sm = str(data_dict.get("dct_description_sm", ""))
        dct_title_s = str(data_dict.get("dct_title_s", ""))
        dct_subject_sm = str(data_dict.get("dct_subject_sm", ""))

        description_lower = dct_description_sm.lower()
        title_lower = dct_title_s.lower()
        subject_lower = dct_subject_sm.lower()

        if (
            ("openindexmaps" not in dct_references_s.lower())
            and (identifier != "stanford-ch237ht4777")
            and ("ch237ht4777" not in dct_source_sm.lower())
        ):
            return False

        logging.debug("OpenIndexMap detected, setting resource class and type.")
        desired_class = ["Maps"]
        desired_type = ["Index maps"]
        topographic_record = any(
            term in " ".join([title_lower, description_lower, subject_lower])
            for term in ["topography", "topographic", "topographical"]
        )

        if "aerial" in description_lower:
            desired_class = ["Imagery"]
        elif topographic_record:
            desired_type.append("Topographic maps")

        changed = False
        current_class = data_dict.get("gbl_resourceClass_sm") or []
        current_type = data_dict.get("gbl_resourceType_sm") or []

        target_class = desired_class
        if desired_class == ["Maps"]:
            target_class = with_unique_items(current_class, desired_class)

        target_type = with_unique_items(current_type, desired_type)

        if data_dict.get("gbl_resourceClass_sm") != target_class:
            data_dict["gbl_resourceClass_sm"] = target_class
            changed = True
        if data_dict.get("gbl_resourceType_sm") != target_type:
            data_dict["gbl_resourceType_sm"] = target_type
            changed = True
        return changed


class WiscoProviderNormalizer:
    @staticmethod
    def normalize(data_dict: Dict) -> bool:
        """Normalize select Wisconsin provider names and prepend provenance text."""
        provider = data_dict.get("schema_provider_s", "")
        if provider not in config["wisco_providers"]:
            return False

        logging.debug(f"Wisco provider identified: {provider}")
        changed = False
        source = data_dict.get("dct_source_sm", [])
        if not isinstance(source, list):
            source = [source] if source else []

        if provider not in source:
            source.append(provider)
            changed = True

        if data_dict.get("dct_source_sm") != source:
            data_dict["dct_source_sm"] = source
            changed = True

        if data_dict.get("schema_provider_s") != ["University of Wisconsin-Madison"]:
            data_dict["schema_provider_s"] = ["University of Wisconsin-Madison"]
            changed = True

        description = data_dict.get("dct_description_sm", [])
        if not isinstance(description, list):
            description = [description] if description else []

        provenance_note = f"Resource provided by {provider}."
        if provenance_note not in description:
            description.insert(0, provenance_note)
            changed = True

        if data_dict.get("dct_description_sm") != description:
            data_dict["dct_description_sm"] = description
            changed = True
        logging.debug(f"Wisco Description now reads: {description}")
        return changed


class RestrictedNoteNormalizer:
    @staticmethod
    def normalize(data_dict: Dict) -> bool:
        """Add a warning note for restricted records."""
        if data_dict.get("dct_accessRights_s") != "Restricted":
            return False

        note = (
            "Warning: This dataset is restricted and you may not be able to access "
            "the resource. Contact the dataset provider or the AGSL for assistance."
        )
        display_notes = data_dict.get("gbl_displayNote_sm")

        if display_notes is None:
            data_dict["gbl_displayNote_sm"] = [note]
            return True
        elif isinstance(display_notes, list):
            if note not in display_notes:
                display_notes.append(note)
                return True
        else:
            data_dict["gbl_displayNote_sm"] = [display_notes, note]
            return True

        return False


class ResourceValueNormalizer:
    @staticmethod
    def normalize(data_dict: Dict) -> bool:
        changed = False

        for field in ["gbl_resourceClass_sm", "gbl_resourceType_sm"]:
            values = data_dict.get(field)
            if not isinstance(values, list):
                continue

            normalized = []
            for value in values:
                cleaned = str(value).strip()
                if cleaned and cleaned not in normalized:
                    normalized.append(cleaned)

            if values != normalized:
                data_dict[field] = normalized
                changed = True

        return changed


class ResourceClassificationNormalizer:
    @staticmethod
    def normalize(data_dict: Dict) -> bool:
        """Fill missing resource class/type using shared classification rules."""
        if data_dict.get("gbl_resourceClass_sm") and data_dict.get(
            "gbl_resourceType_sm"
        ):
            return False

        resource_class, resource_type = (
            ResourceClassifier.determine_resource_class_and_type(data_dict)
        )
        changed = False

        if data_dict.get("gbl_resourceClass_sm") != resource_class:
            data_dict["gbl_resourceClass_sm"] = resource_class
            changed = True
        if data_dict.get("gbl_resourceType_sm") != resource_type:
            data_dict["gbl_resourceType_sm"] = resource_type
            changed = True

        return changed


class TitleTransliterationNormalizer:
    FIELD = "agsl_title_transliterated_s"
    ICU_TRANSFORM = "Any-Latin; Latin-ASCII"
    MAX_CACHE_SIZE = 10_000
    TRANSIENT_WARNING_LIMIT = 1
    LEADING_NON_ALNUM = " \t\r\n\v\f!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"

    _cache = {}
    _disabled = False
    _transient_failures = 0

    @classmethod
    def normalize(cls, data_dict: Dict) -> bool:
        title = str(data_dict.get("dct_title_s", ""))
        transliterated = cls.transliterate(title, record_id=data_dict.get("id"))
        current = data_dict.get(cls.FIELD)

        if transliterated:
            if current != transliterated:
                data_dict[cls.FIELD] = transliterated
                return True
        else:
            if cls.FIELD in data_dict:
                data_dict.pop(cls.FIELD, None)
                return True

        return False

    @classmethod
    def transliterate(
        cls, title: str, record_id: Optional[str] = None
    ) -> Optional[str]:
        if not title or not cls.needs_transliteration(title):
            return None
        if title in cls._cache:
            return cls._cache[title]
        if cls._disabled:
            return None

        record_label = f" for record {record_id}" if record_id else ""

        try:
            result = subprocess.run(
                ["uconv", "-x", cls.ICU_TRANSFORM],
                input=title.replace("\n", " ") + "\n",
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                universal_newlines=True,
                encoding="utf-8",
                timeout=5,
                check=False,
            )
        except FileNotFoundError:
            logging.warning(
                f"Title transliteration requires the ICU 'uconv' binary to be installed and on PATH{record_label}."
            )
            cls.disable()
            return None
        except (OSError, subprocess.SubprocessError) as exc:
            cls._transient_failures += 1
            if cls._transient_failures <= cls.TRANSIENT_WARNING_LIMIT:
                logging.warning(f"uconv transliteration failed{record_label}: {exc}")
            else:
                logging.info(f"uconv transliteration failed{record_label}: {exc}")
            return None

        stdout = result.stdout
        transliterated = " ".join(stdout.split())
        if not transliterated or transliterated == title:
            cls.trim_cache()
            cls._cache[title] = None
            return None

        cls.trim_cache()
        cls._cache[title] = transliterated
        return transliterated

    @classmethod
    def disable(cls) -> None:
        cls._disabled = True

    @classmethod
    def trim_cache(cls) -> None:
        if len(cls._cache) >= cls.MAX_CACHE_SIZE:
            cls._cache.clear()

    @staticmethod
    def needs_transliteration(title: str) -> bool:
        normalized = title.lstrip(TitleTransliterationNormalizer.LEADING_NON_ALNUM)
        if not normalized:
            return False
        first_char = normalized[0]
        if first_char.isdigit():
            return False

        return "LATIN" not in unicodedata.name(first_char, "")


class MetadataNormalizer:
    @staticmethod
    def normalize_document(data_dict: Dict) -> bool:
        changed = False
        changed = ResourceValueNormalizer.normalize(data_dict) or changed
        changed = StanfordSpatialNormalizer.normalize(data_dict) or changed
        changed = OpenIndexMapsNormalizer.normalize(data_dict) or changed
        changed = WiscoProviderNormalizer.normalize(data_dict) or changed
        changed = RestrictedNoteNormalizer.normalize(data_dict) or changed
        changed = ResourceClassificationNormalizer.normalize(data_dict) or changed
        changed = TitleTransliterationNormalizer.normalize(data_dict) or changed
        return changed


def write_json_atomically(path: Path, data) -> None:
    """Write JSON without truncating the destination on partial failures."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf8",
        dir=path.parent,
        delete=False,
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        json.dump(data, tmp_file, indent=2)
        tmp_file.write("\n")
        tmp_file.flush()
        os.fsync(tmp_file.fileno())

    try:
        with open(tmp_path, encoding="utf8") as check_file:
            json.load(check_file)
        if tmp_path.stat().st_size == 0:
            raise ValueError(f"Refusing to replace {path} with an empty JSON file.")
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def iter_json_files(rootdir: Path) -> Iterable[Path]:
    for path in rootdir.rglob("*.json"):
        if path.name != "layers.json":
            yield path


def normalize_directory(rootdir: Path, schema_version: str = "Aardvark") -> int:
    updated = 0
    scanned = 0

    logging.info(
        f"Starting normalization in {rootdir} for schema version {schema_version}."
    )

    for path in iter_json_files(rootdir):
        scanned += 1
        if scanned % 1000 == 0:
            logging.info(f"Scanned {scanned} files; updated {updated} so far.")
        try:
            with open(path, encoding="utf8") as file:
                data = json.load(file)
        except FileNotFoundError:
            logging.error(f"File not found: {path}")
            continue
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON in file: {path}")
            continue

        records = data if isinstance(data, list) else [data]
        changed = False

        for record in records:
            if not isinstance(record, dict):
                continue

            record_schema = record.get("gbl_mdVersion_s") or record.get(
                "geoblacklight_version"
            )
            if record_schema != schema_version:
                continue

            changed = MetadataNormalizer.normalize_document(record) or changed

        if changed:
            write_json_atomically(path, data)
            updated += 1
            if updated <= 10 or updated % 100 == 0:
                logging.info(f"Updated {path}")

    logging.info(f"Finished scanning {scanned} files; updated {updated}.")
    return updated


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Normalize harvested Aardvark metadata in place."
    )
    parser.add_argument(
        "rootdir",
        type=Path,
        nargs="?",
        default=(CONFIG_DIR / os.getenv("OGM_PATH", config["paths"]["ogm_path"])).resolve(),
        help="Root directory of harvested JSON files",
    )
    parser.add_argument(
        "--schema_version",
        type=str,
        default=os.getenv("SCHEMA_VERSION", "Aardvark"),
        help="Only normalize records matching this schema version",
    )

    logfile = config["logging"]["logfile"]
    if not os.path.isabs(logfile):
        logfile = str((CONFIG_DIR / logfile).resolve())
    level = getattr(logging, config["logging"]["level"].upper(), logging.ERROR)
    os.makedirs(os.path.dirname(logfile), exist_ok=True)
    logging.basicConfig(
        filename=logfile,
        level=level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

    args = parser.parse_args()
    updated = normalize_directory(args.rootdir, args.schema_version)
    print(f"Normalized {updated} files.")

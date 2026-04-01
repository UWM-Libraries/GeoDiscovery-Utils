import logging
from typing import Dict, List, Optional, Tuple


class ResourceClassifier:
    @staticmethod
    def determine_resource_class_and_type(
        data_dict: Dict,
        resource_class_default: Optional[str] = None,
        resource_type_default: Optional[str] = None,
    ) -> Tuple[List[str], List[str]]:
        """Classify a record into Aardvark resource class and type."""

        def append_if_not_exists(lst: List[str], item: str) -> None:
            if item not in lst:
                lst.append(item)

        def contains_any(text: str, terms: List[str]) -> bool:
            return any(term in text for term in terms)

        def map_related(title: str, description: str, subject: str) -> bool:
            return (
                "relief" in description
                or "map" in description
                or "maps" in subject
                or "plan" in title
                or "map" in title
                or "topographic" in title
            )

        def with_unique_items(items: List[str], additions: List[str]) -> List[str]:
            result = list(items)
            for item in additions:
                if item not in result:
                    result.append(item)
            return result

        resource_class = data_dict.get("gbl_resourceClass_sm") or []
        resource_type = data_dict.get("gbl_resourceType_sm") or []

        if not isinstance(resource_class, list):
            resource_class = [resource_class] if resource_class else []
        if not isinstance(resource_type, list):
            resource_type = [resource_type] if resource_type else []

        if resource_class and resource_type:
            logging.debug("Resource class and type already determined.")
            return resource_class, resource_type

        title = str(data_dict.get("dct_title_s", ""))
        format_value = str(data_dict.get("dct_format_s", ""))
        description = str(data_dict.get("dct_description_sm", ""))
        subject = str(data_dict.get("dct_subject_sm", ""))
        publisher = str(data_dict.get("dct_publisher_sm", ""))
        identifier = str(data_dict.get("id", ""))
        references = str(data_dict.get("dct_references_s", ""))
        source = str(data_dict.get("dct_source_sm", ""))

        title_lower = title.lower()
        description_lower = description.lower()
        subject_lower = subject.lower()
        publisher_lower = publisher.lower()
        references_lower = references.lower()
        source_lower = source.lower()

        aerial_photo = "aerial photo" in title_lower
        iiif_reference = "iiif" in references_lower
        openindexmaps_reference = "openindexmaps" in references_lower
        map_like_record = map_related(title_lower, description_lower, subject_lower)
        topographic_record = contains_any(
            " ".join([title_lower, description_lower, subject_lower]),
            ["topography", "topographic", "topographical"],
        )
        parcel_record = contains_any(
            " ".join([title_lower, description_lower, subject_lower]),
            ["parcel", "parcels"],
        )
        vector_download_reference = contains_any(
            references_lower,
            ["shapefile", "geodatabase", "_shp.zip", "_gdb.zip", ".shp", ".gdb"],
        )

        if (
            openindexmaps_reference
            or (identifier == "stanford-ch237ht4777")
            or ("ch237ht4777" in source_lower)
        ):
            logging.debug("OpenIndexMap detected, setting resource class and type.")
            resource_class = with_unique_items(resource_class, ["Maps"])
            resource_type = with_unique_items(resource_type, ["Index maps"])
            if "aerial" in description_lower:
                resource_class = ["Imagery"]
            elif topographic_record:
                resource_type = with_unique_items(resource_type, ["Topographic maps"])
            return resource_class, resource_type

        logging.debug(
            "Classifying %s with title=%r format=%r",
            identifier,
            title,
            format_value,
        )

        if aerial_photo:
            logging.debug("Aerial photography detected from title.")
            return ["Imagery"], ["Aerial photographs"]

        if "sanborn" in publisher_lower:
            logging.debug("Sanborn map detected, setting resource class and type.")
            append_if_not_exists(resource_class, "Maps")
            append_if_not_exists(resource_type, "Fire insurance maps")
            return resource_class, resource_type

        if "topographical map" in title_lower:
            logging.debug(
                "Topographical map detected, setting resource class and type."
            )
            append_if_not_exists(resource_class, "Maps")
            append_if_not_exists(resource_type, "Topographic maps")
            return resource_class, resource_type

        if "aeronautical" in title_lower:
            logging.debug(
                "Aeronautical charts detected, setting resource class and type."
            )
            append_if_not_exists(resource_class, "Maps")
            append_if_not_exists(resource_type, "Aeronautical charts")
            return resource_class, resource_type

        if iiif_reference:
            logging.debug("IIIF Map detected, setting resource class and type.")
            append_if_not_exists(resource_class, "Maps")
            if aerial_photo or ("aerial photo" in description_lower):
                logging.debug("IIIF aerial photography detected.")
                append_if_not_exists(resource_type, "Aerial photographs")
            else:
                append_if_not_exists(resource_type, "Digital maps")
            return resource_class, resource_type

        if format_value in ["GeoTIFF", "TIFF"]:
            if map_like_record:
                logging.debug(
                    "GeoTIFF or TIFF format with map-related description or subject detected."
                )
                append_if_not_exists(resource_class, "Maps")
                append_if_not_exists(resource_type, "Digital maps")
                return resource_class, resource_type

            if aerial_photo:
                logging.debug("Aerial photography detected from raster title.")
                append_if_not_exists(resource_type, "Aerial photographs")
                resource_class = ["Imagery"]

            logging.debug(
                "GeoTIFF or TIFF format detected, setting resource class to Datasets."
            )
            return ["Datasets"], resource_type

        if (
            format_value
            in [
                "Shapefile",
                "ArcGrid",
                "GeoDatabase",
                "Geodatabase",
                "Arc/Info Binary Grid",
            ]
            or "csdgm" in references_lower
            or "arcgis#" in references_lower
        ):
            logging.debug("Setting resource class to Datasets based on format.")
            resource_class = ["Datasets"]
            if aerial_photo:
                logging.debug("Aerial photography detected from dataset title.")
                return ["Imagery"], ["Aerial photographs"]
            return resource_class, resource_type

        if parcel_record and (
            format_value
            in ["Shapefile", "GeoDatabase", "Geodatabase", "Multiple Formats"]
            or vector_download_reference
        ):
            logging.debug("Parcel-style vector dataset detected.")
            return ["Datasets"], ["Polygon data", "Cadastral maps"]

        if format_value == "":
            if map_like_record:
                logging.debug(
                    "Empty format with map-related description or subject detected."
                )
                append_if_not_exists(resource_class, "Maps")
                return resource_class, resource_type

            logging.debug("Empty format, setting resource class to Other.")
            return ["Other"], resource_type

        if format_value in ["ArcGRID", "IMG"] or contains_any(
            description_lower,
            [
                "dem",
                "dsm",
                "digital elevation model",
                "digital terrain model",
                "digital surface model",
                "arc-second",
                "raster dataset",
            ],
        ):
            logging.debug("Elevation or other non-Imagery Raster Detected.")
            append_if_not_exists(resource_class, "Datasets")
            append_if_not_exists(resource_type, "Raster data")
            return resource_class, resource_type

        if map_like_record:
            logging.debug("Map-related description or subject detected.")
            return ["Maps"], resource_type

        logging.debug("Setting default resource class and type.")
        fallback_class = resource_class_default or "Datasets"
        resource_class = [fallback_class]

        if resource_type_default:
            resource_type = [resource_type_default]
        else:
            resource_type = []

        return resource_class, resource_type

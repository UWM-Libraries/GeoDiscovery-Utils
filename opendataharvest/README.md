# opendataharvest

This standalone copy mirrors the current GeoDiscovery `opendataharvest` workflow more closely.

## Layout

- `config.yaml` now treats `tmp/opengeometadata` as the local OGM root
- DCAT harvest output is written under `tmp/opengeometadata/opendataharvest`
- local support files still live under `data/`

## Scripts

- `DCAT_Harvester.py`: harvest DCAT records into the configured output directory
- `gbl_to_aardvark.py`: convert legacy OGM 1.0 repositories only when they actually need conversion
- `normalize.py`: normalize harvested Aardvark JSON in place
- `convert.py`: convert legacy GeoBlacklight 1.0 JSON to Aardvark
- `classify.py`: shared resource class/type classification rules used by normalization

## Notes

- This utility copy keeps standalone-relative paths instead of the Rails app paths used in GeoDiscovery.
- `OGM_PATH` can still override `paths.ogm_path`.
- `output_md/` is no longer the default OGM root; use `tmp/opengeometadata/` for local mirrors and harvest output.

# Make sure that every dataset has a matching metadata record and vice versa:

from pathlib import Path

FILE_SERVER_PATH = Path(r"S:\GeoBlacklight\web")

metadata_path = FILE_SERVER_PATH / "metadata"
public_path = FILE_SERVER_PATH / "public"
restricted_uw_system_path = FILE_SERVER_PATH / "restricted-uw-system"

# Check to make sure a folder exists for each metdata record in the metadata directory
for file in metadata_path.iterdir():
    arkid = file.name[:11] # would be more accurate with regex
    check_public_path = public_path / arkid
    check_restricted_uw_system_path = restricted_uw_system_path / arkid
    if check_public_path.exists() and check_public_path.is_dir():
        continue
    elif check_restricted_uw_system_path.exists() and check_restricted_uw_system_path.is_dir():
        continue
    else:
        print(f'{arkid} was not found in public or restricted directories')

# Check to make sure that all directories in public have corresponding metadata:
for data_dir in public_path.iterdir():
    if data_dir.is_dir():
        arkid = data_dir.name
    else:
        print(f'{data_dir} is not a directory!')

    check_metadata_path = metadata_path / f'{arkid}_ISO.xml'
    if check_metadata_path.exists():
        continue
    else:
        print(f"No metadata for {arkid} from public directory")

# Check to make sure that all directories in restricted have corresponding metadata:
for data_dir in restricted_uw_system_path.iterdir():
    if data_dir.is_dir():
        arkid = data_dir.name
    else:
        print(f'{data_dir} is not a directory!')

    check_metadata_path = metadata_path / f'{arkid}_ISO.xml'
    if check_metadata_path.exists():
        continue
    else:
        print(f"No metadata for {arkid} from restricted directory")

# Do some math:
metadata_sum = sum(1 for _ in metadata_path.iterdir())
public_sum = sum(1 for _ in public_path.iterdir())
restricted_uw_system_sum = sum(1 for _ in restricted_uw_system_path.iterdir())
total = public_sum + restricted_uw_system_sum
print(f'There are {str(metadata_sum)} metadata records')
print(f'There are {str(total)} datasets')

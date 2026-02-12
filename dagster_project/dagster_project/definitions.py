from dagster import Definitions, load_assets_from_modules
from dagster_project.assets import github_raw

all_assets = load_assets_from_modules([github_raw])

defs = Definitions(assets=all_assets)
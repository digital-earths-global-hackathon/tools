from pystac import Catalog, Item, Asset, MediaType
import datetime
import pandas as pd
import numpy as np
import yaml
from collections import defaultdict
from typing import Union
from pathlib import Path
import intake
import intake_tools


class catalog_generator:
    defaults: dict
    catalog: any

    def __init__(self) -> None:
        self.read_defaults()
        self.create_basic_catalog()

    def read_defaults(self, source: Union[Path, str] = Path(__file__).parent/"defaults.yaml"):
        with open(source) as source_file:
            self.defaults = yaml.safe_load(source_file)

    def create_basic_catalog(self) -> None:
        self.catalog = Catalog(**self.defaults["catalog_properties"])

    def parse_file(self, filename):
        entries = intake.open_catalog(filename)
        simulations = self._sort_by_simulation_id(entries)
        for name, sim in simulations.items():
            item = self._gen_item(name, sim[0])
            self.catalog.add_item(item)

    def _gen_item(self, name, sim):
        time_range = __class__.get_time_range(sim)
        item = Item(
            id=name,
            geometry=None,  # Replace with actual geometry, if available
            bbox=None,  # Replace with actual bounding box, if available
            datetime=None,  # Replace with actual datetime, if applicable
            **time_range,
            properties=sim.describe()["metadata"],
        )
        self._add_assets(sim, item)
        return item

    def _add_assets(self, sim, item):
        for params in intake_tools.iterate_user_parameters(sim):
            asset = self._gen_asset(sim, params)
            item.add_asset(key=f"data_{asset.title}", asset=asset)

    def _gen_asset(self, sim, params) -> Asset:
        asset_href = sim(**params).urlpath
        param_string = "_".join([str(x) for x in list(params.values())])
        asset = Asset(
            href=asset_href,
            media_type=MediaType.ZARR,
            title=param_string,
            description="md",
            extra_fields=self.defaults["asset_properties"],
        )
        return asset

    @staticmethod
    def get_time_range(entry):
        ds = entry.to_dask()
        convert_time = __class__.convert_time
        return dict(
            start_datetime=convert_time(ds.time.values[0]),
            end_datetime=convert_time(ds.time.values[-1]),
        )

    @staticmethod
    def convert_time(time: np.datetime64) -> datetime.datetime:
        return pd.to_datetime(time).to_pydatetime()

    def _sort_by_simulation_id(self, entries: dict) -> dict:
        simulations = defaultdict(list)
        for name, entry in entries.items():
            simulations[entry.describe()["metadata"]["simulation_id"]].append(entry)
        return simulations

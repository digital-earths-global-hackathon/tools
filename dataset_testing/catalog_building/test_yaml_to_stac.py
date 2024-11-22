import intake_to_stac
from pystac import CatalogType
import intake


def test_cgen():
    cgen = intake_to_stac.catalog_generator()
    cgen.parse_file("sample_dataset.yaml")
    cgen.catalog.normalize_and_save(
        root_href="./", catalog_type=CatalogType.SELF_CONTAINED
    )


def test_read():
    intake.open_stac_catalog("catalog.json")["ngc4008"]["data_P1D_0"].to_dask()

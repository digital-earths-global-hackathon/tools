import intake_to_stac
from pystac import CatalogType
import intake
from pathlib import Path
import tempfile

def test_cgen():
    cgen = intake_to_stac.catalog_generator()
    cgen.parse_file(Path(__file__).parent / Path("sample_dataset.yaml"))
    with tempfile.TemporaryDirectory() as root :
        cgen.catalog.normalize_and_save(
            root_href=root, catalog_type=CatalogType.SELF_CONTAINED
        )
        read(root)


def read(root):
    intake.open_stac_catalog((Path(root)/Path("catalog.json")).as_posix())["ngc4008"]["data_P1D_0"].to_dask()

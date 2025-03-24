import intake


def test_eerie_cat():
    baseurl = "https://eerie.cloud.dkrz.de/datasets"
    dataset = "ifs-amip-tco1279.hist.v20240901.atmos.native.2D_24h"
    stac_item = intake.open_stac_item("/".join([baseurl, dataset, "stac"]))
    ds = stac_item["eerie-cloud"].to_dask()
    tmean = ds.mean2t.isel(time=2).mean().values
    assert tmean > 280
    assert tmean < 290

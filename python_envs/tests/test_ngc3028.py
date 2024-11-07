import intake

def test_basic_access():
    date="2020-10-28"
    cat = intake.open_catalog("https://data.nextgems-h2020.eu/online.yaml")
    ds = cat["ICON.ngc3028"].to_dask()
    tas_mean = ds.tas.sel(time=date).mean (dim='cell').values
    assert ( tas_mean > 273.15)
    assert ( tas_mean < 300)

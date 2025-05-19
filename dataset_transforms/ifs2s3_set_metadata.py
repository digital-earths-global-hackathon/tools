import zarr

storage_options = {
    "endpoint_url": "https://s3.eu-dkrz-1.dkrz.cloud",
    "key": "ifs-fesom",
    "secret": "***"
}
s3prefix = "s3://wrcp-hackathon/data/IFS-FESOM"


for nside in [128, 2048]:
    z = zarr.open(s3prefix + f"/hourly_healpix{nside}.zarr", mode="a",
                  storage_options=storage_options,
                  zarr_version=2)

    z["rlut"].attrs["units"] = "W m-2"
    z["rsut"].attrs["units"] = "W m-2"
    z["pr"].attrs["units"] = "kg m-2 s-1"
    z["hflsd"].attrs["units"] = "W m-2"
    z["hfssd"].attrs["units"] = "W m-2"
    z["rlutcs"].attrs["units"] = "W m-2"
    z["rlus"].attrs["units"] = "W m-2"
    z["rluscs"].attrs["units"] = "W m-2"
    z["rlds"].attrs["units"] = "W m-2"
    z["rsdt"].attrs["units"] = "W m-2"
    z["rsus"].attrs["units"] = "W m-2"
    z["rsuscs"].attrs["units"] = "W m-2"
    z["rsds"].attrs["units"] = "W m-2"
    z["rsdscs"].attrs["units"] = "W m-2"
    z["prs"].attrs["units"] = "kg m-2 s-1"
    z["tauu"].attrs["units"] = "N m-2"
    z["tauu"].attrs["units"] = "N m-2"
    z["swe"].attrs["units"] = "mm"
    z["mrso"].attrs["units"] = "m3 m-3"
    z["qall"].attrs["units"] = "kg kg-1"

    zarr.consolidate_metadata(z.store)


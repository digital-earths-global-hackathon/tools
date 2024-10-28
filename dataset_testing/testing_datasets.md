
# Playing with the nextGEMS cycle4 data

Load your favorite python version, then create a venv for the easy life

```bash
python -m venv create easylife
. easylife/bin/activate
pip install --upgrade intake==0.7.0 easygems aiohttp jinja2 intake-xarray zarr pip setuptools ipykernel
python -m ipykernel install --name easylife
python
```
in python run

```python
import intake
import easygems.healpix as egh
import matplotlib.pyplot as plt

date="2024-10-28"

cat = intake.open_catalog("https://data.nextgems-h2020.eu/online.yaml")
ds = cat["ICON.ngc4008"](zoom=5).to_dask()
egh.healpix_show(ds["tas"].sel(time=date), cmap="inferno")
plt.title (f"Surface air temperature {date}")
plt.savefig(f"tas_{date}.png")
```

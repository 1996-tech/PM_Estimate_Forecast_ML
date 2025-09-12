import xarray as xr
import numpy as np
import pandas as pd
from pyproj import Proj
from pathlib import Path
from calendar import monthrange

# -----------------------------
# 1. Setup paths
# -----------------------------
base_dir = Path("/mnt/AIRNAS1/airpact/AIRRUN/2024")
out_dir = base_dir / "hourly_L01_PM2.5"
out_dir.mkdir(exist_ok=True)

# -----------------------------
# 2. Build full list of dates for 2024
# -----------------------------
days = []
for month in range(1, 13):  # Jan–Dec
    ndays = monthrange(2024, month)[1]  # number of days in month
    for day in range(1, ndays + 1):
        days.append(f"2024{month:02d}{day:02d}00")

# -----------------------------
# 3. Decode time from TFLAG
# -----------------------------
def decode_time(tflag):
    yyyyddd, hhmmss = tflag
    year = int(str(yyyyddd)[:4])
    day_of_year = int(str(yyyyddd)[4:])
    hhmmss_str = str(hhmmss).zfill(6)
    hour = int(hhmmss_str[:2])
    minute = int(hhmmss_str[2:4])
    second = int(hhmmss_str[4:6])
    return pd.to_datetime(f"{year}-{day_of_year}", format="%Y-%j") + pd.Timedelta(
        hours=hour, minutes=minute, seconds=second
    )

# -----------------------------
# 4. Loop over all days in 2024
# -----------------------------
for d in days:
    aconc_file = base_dir / d / "CCTM" / f"ACONC_{d[:8]}.ncf"
    if not aconc_file.exists():
        print(f"⚠ Missing {aconc_file}, skipping")
        continue

    print(f" Processing {aconc_file}")
    ds = xr.open_dataset(aconc_file)

    # --- Select Layer 01 (surface) ---
    ds_surf = ds.isel(LAY=0)

    # --- Build PM2.5 from components ---
    pm25 = (
        ds_surf["ASO4J"] + ds_surf["ASO4I"] +   # sulfate
        ds_surf["ANO3J"] + ds_surf["ANO3I"] +   # nitrate
        ds_surf["ANH4J"] + ds_surf["ANH4I"] +   # ammonium
        ds_surf["AECJ"]  + ds_surf["AECI"]  +   # elemental carbon
        ds_surf["APOCJ"] + ds_surf["APOCI"] +   # primary organic carbon
        ds_surf["AORGCJ"] +                     # secondary organic carbon
        ds_surf["AOTHRI"] + ds_surf["AOTHRJ"]   # other PM
    )
    pm25.name = "PM25_TOT"
    pm25.attrs["units"] = "µg/m³"
    pm25.attrs["long_name"] = "Total PM2.5 mass concentration (Layer 01, surface)"

    # --- Decode time ---
    times = [decode_time(tf.values) for tf in ds["TFLAG"][:, 0, :]]
    pm25 = pm25.assign_coords(time=("TSTEP", times))

    # --- Compute lat/lon coordinates ---
    nx = ds.NCOLS
    ny = ds.NROWS
    x = ds.XORIG + (np.arange(nx) + 0.5) * ds.XCELL
    y = ds.YORIG + (np.arange(ny) + 0.5) * ds.YCELL

    p = Proj(
        proj="lcc",
        lat_1=float(ds.P_ALP),
        lat_2=float(ds.P_BET),
        lat_0=float(ds.YCENT),
        lon_0=float(ds.XCENT),
        a=6370000, b=6370000
    )

    lon, lat = np.meshgrid(x, y)
    lon, lat = p(lon, lat, inverse=True)

    pm25 = pm25.assign_coords(
        lat=(("ROW", "COL"), lat),
        lon=(("ROW", "COL"), lon)
    )

    # --- Save daily NetCDF ---
    out_nc = out_dir / f"PM25_surface_reconstructed_{d[:8]}.nc"
    pm25.to_netcdf(out_nc)

    print(f" Saved {out_nc}")

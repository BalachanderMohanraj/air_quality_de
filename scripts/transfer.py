import pandas as pd
import duckdb
import numpy as np
from datetime import datetime, timezone
csv_file = r"data\Raw\locations.csv"
df = pd.read_csv(csv_file)
rename_map = {
    "bounds[0]": "bounds_min_long",
    "bounds[1]": "bounds_min_lat",
    "bounds[2]": "bounds_max_long",
    "bounds[3]": "bounds_max_lat",
    "coordinates.latitude": "latitude",
    "coordinates.longitude": "longitude",
    "country.code": "country_code",
    "country.id": "country_id",
    "country.name": "country_name",
    "datetimeFirst": "datetime_first",
    "datetimeFirst.local": "datetime_first_local",
    "datetimeFirst.utc": "datetime_first_utc",
    "datetimeLast": "datetime_last",
    "datetimeLast.local": "datetime_last_local",
    "datetimeLast.utc": "datetime_last_utc",
    "distance": "distance",
    "id": "location_id",
    "instruments[0].id": "instrument_0_id",
    "instruments[0].name": "instrument_0_name",
    "instruments[1].id": "instrument_1_id",
    "instruments[1].name": "instrument_1_name",
    "isMobile": "is_mobile",
    "isMonitor": "is_monitor",
    "licenses": "licenses",
    "licenses[0].attribution.name": "license_attr_name",
    "licenses[0].attribution.url": "license_attr_url",
    "licenses[0].dateFrom": "license_date_from",
    "licenses[0].dateTo": "license_date_to",
    "licenses[0].id": "license_id",
    "licenses[0].name": "license_name",
    "locality": "locality",
    "name": "location_name",
    "owner.id": "owner_id",
    "owner.name": "owner_name",
    "provider.id": "provider_id",
    "provider.name": "provider_name",
    "timezone": "timezone",
}
for i in range(8):
    rename_map.update({
        f"sensors[{i}].id": f"sensor_{i}_id",
        f"sensors[{i}].name": f"sensor_{i}_name",
        f"sensors[{i}].parameter.displayName": f"sensor_{i}_display_name",
        f"sensors[{i}].parameter.id": f"sensor_{i}_param_id",
        f"sensors[{i}].parameter.name": f"sensor_{i}_param_name",
        f"sensors[{i}].parameter.units": f"sensor_{i}_param_units",
    })
df = df.rename(columns=rename_map)
df["license_date_from"] = pd.to_datetime(df["license_date_from"], errors='coerce', dayfirst=True).dt.date
df["license_date_to"]   = pd.to_datetime(df["license_date_to"], errors='coerce', dayfirst=True).dt.date
df["datetime_first_local"] = pd.to_datetime(df["datetime_first_local"], errors='coerce', utc=True)
df["datetime_first_utc"]   = pd.to_datetime(df["datetime_first_utc"], errors='coerce', utc=True)
df["datetime_last_local"]  = pd.to_datetime(df["datetime_last_local"], errors='coerce', utc=True)
df["datetime_last_utc"]    = pd.to_datetime(df["datetime_last_utc"], errors='coerce', utc=True)
df["run_id"] = np.arange(1, len(df) + 1)
df["ingestion_id"] = np.arange(1, len(df) + 1)   
df["updated_ingestion_id"] = df["ingestion_id"]
df["ingestion_datetime"] = datetime.now(timezone.utc)
df["updated_ingestion_datetime"] = datetime.now(timezone.utc)
df["source_system"] = "openaq"
con = duckdb.connect("mydb.duckdb")
table_cols = [col[1] for col in con.execute("PRAGMA table_info(scr_locations)").fetchall()]
for col in table_cols:
    if col not in df.columns:
        df[col] = None
con.register("df_view", df)
cols = ", ".join(df.columns)
con.execute(f"INSERT INTO scr_locations ({cols}) SELECT {cols} FROM df_view")
print("✅ Data inserted into scr_locations successfully.")
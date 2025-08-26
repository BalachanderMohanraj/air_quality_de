import duckdb
con = duckdb.connect("mydb.duckdb")
result = con.execute("SELECT * FROM dl_locations").fetchdf()
print(result)
print(result.columns.tolist())
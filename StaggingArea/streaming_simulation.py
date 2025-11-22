import duckdb

con = duckdb.connect("music.duckdb")

result = con.execute("SELECT * FROM sqlite_master LIMIT 5").fetchdf()

print(df)
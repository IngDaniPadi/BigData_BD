import duckdb

con = duckdb.connect("music.duckdb")

result = con.execute("SELECT * FROM users LIMIT 5").fetchdf()

print(result)
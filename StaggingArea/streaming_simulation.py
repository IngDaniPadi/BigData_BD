import duckdb

con = duckdb.connect("music.duckdb")

df = con.execute("SELECT COUNT(*) FROM user_top_tracks").fetchall()

print(df)
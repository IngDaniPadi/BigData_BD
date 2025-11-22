import duckdb

con = duckdb.connect("music.duckdb")

tables_name = [t[0] for t in con.execute("SHOW TABLES").fetchall()] 

for name in tables_name:
    print(f"Estamos usando la tabla {name}")
import marimo as mo
import duckdb

con = duckdb.connect("open_library_pipeline.duckdb")

@mo.cell
def _():
    return con.execute("""
        SELECT COUNT(*) AS spanish_books
        FROM open_library_data__books
        WHERE 'spa' = ANY(languages)  -- adjust once you inspect schema
    """).df()
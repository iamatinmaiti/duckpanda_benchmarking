import marimo

__generated_with = "0.21.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import marimo as mo
    import pandas as pd
    from pathlib import Path

    return Path, duckdb, mo, pd


@app.cell
def _(Path):
    project_root = Path(__file__).resolve().parent
    hdfs_dir = project_root / "hdfs"
    parquet_files = sorted(hdfs_dir.glob("*.parquet"))
    parquet_glob = str(hdfs_dir / "*.parquet")
    duckdb_path = project_root / "benchmark.duckdb"
    return duckdb_path, parquet_files, parquet_glob


@app.cell
def _(duckdb, duckdb_path, parquet_files, parquet_glob):
    if not parquet_files:
        preview_df = None
        row_count = 0
        schema_df = None
        view_name = "parquet_parts"
    else:
        conn = duckdb.connect(str(duckdb_path))
        view_name = "parquet_parts"
        parquet_glob_sql = parquet_glob.replace("'", "''")
        conn.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS "
            f"SELECT * FROM read_parquet('{parquet_glob_sql}')"
        )
        row_count = conn.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        preview_df = conn.execute(f"SELECT * FROM {view_name} LIMIT 10").df()
        schema_df = conn.execute(f"DESCRIBE SELECT * FROM {view_name}").df()
        conn.close()
    return preview_df, row_count, schema_df, view_name


@app.cell
def _(mo, parquet_files, row_count, view_name):
    mo.md(f"""
    # DuckDB reading your parquet files

    DuckDB view: `{view_name}`

    - Parquet files found: **{len(parquet_files)}**
    - Total rows across all parquet files: **{row_count}**

    Use this SQL in DuckDB:

    ```sql
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT *
    FROM read_parquet('hdfs/*.parquet');

    SELECT * FROM {view_name} LIMIT 10;
    ```
    """)
    return


@app.cell
def _(mo, preview_df, schema_df):
    if schema_df is None:
        mo.md("No parquet files found in `hdfs/`.")
    else:
        mo.vstack(
            [
                mo.md("## DuckDB schema"),
                mo.ui.table(schema_df),
                mo.md("## DuckDB preview"),
                mo.ui.table(preview_df),
            ]
        )
    return


@app.cell
def _(parquet_files, pd):
    if not parquet_files:
        pandas_df = None
        pandas_shape = (0, 0)
    else:
        pandas_df = pd.read_parquet(parquet_files[0])
        pandas_shape = pandas_df.shape
    return pandas_df, pandas_shape


@app.cell
def _(mo, pandas_df, pandas_shape):
    if pandas_df is None:
        mo.md("No sample parquet available for pandas.")
    else:
        mo.vstack(
            [
                mo.md(f"## Pandas sample shape\n\n`{pandas_shape}`"),
                mo.ui.table(pandas_df.head(10)),
            ]
        )
    return


if __name__ == "__main__":
    app.run()

"""
Utility functions for handling SQL databases.
"""
from pathlib import Path
from typing import Iterable, Optional
from sqlite3 import Connection

from pandas import DataFrame

_this_file: Path = Path(__file__)

PATH_TO_DB:      Path = _this_file.parents[1] / "data/db.db"
PATH_TO_SAMPLES: Path = _this_file.parents[1] / "data/samples.db"

def _connect_to_any_db(db_name: str) -> tuple[Connection, Path]:
    """
    Connects to any database in the 'data' directory.
    """
    from sqlite3 import connect
    from pathlib import Path

    db_name = db_name.removesuffix('.db')
    path_to_db: Path = _this_file.parents[1] / f"data/{db_name}.db"

    return connect(path_to_db), path_to_db

def connect_to_db() -> Connection:
    """
    Connects to the database.
    """
    return _connect_to_any_db('db')[0]

def connect_to_samples() -> Connection:
    """
    Connects to the database of samples.
    """
    return _connect_to_any_db('samples')[0]

def get_table_names(db_name: str) -> list[str]:
    """
    Lists the names of all available tables in the database.
    """
    connection = _connect_to_any_db(db_name)[0]
    cursor = connection.cursor()
    return [
        table_info[1] \
        for table_info in cursor.execute("PRAGMA table_list").fetchall() \
        if not table_info[1].startswith('sqlite_')
    ]

def get_table_fields(db_name: str, table_name: str) -> list[str]:
    """
    Lists the available fields for the given table.
    """
    assert table_name in get_table_names(db_name)

    connection = _connect_to_any_db(db_name)[0]
    cursor = connection.cursor()
    return [
        table_info[1] \
        for table_info in cursor.execute(f"PRAGMA table_info(name)").fetchall()
    ]

def remove_table(db_name: str, table_name: str) -> None:
    """
    Removes the designated table from the database.
    """
    assert table_name in get_table_names(db_name)

    connection = _connect_to_any_db(db_name)[0]
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE {table_name}")

def add_table_to_db(
    db_name: str,
    table_name: str,
    table_fields: Iterable[str],
) -> Connection:
    """
    Creates a table with the given name and fields.
    """
    assert table_name not in get_table_names(db_name)

    sql: str = f"CREATE TABLE {table_name}(" + ", ".join(table_fields) + ")" 
    
    connection = _connect_to_any_db(db_name)[0]
    cursor = connection.cursor()
    cursor.execute(sql)

def write_to_db(
    db_name: str,
    table_name: str,
    df: DataFrame,
    replace: bool = True,
    chunksize: int = 1000,
) -> None:
    """
    Writes a Pandas dataframe to the selected table.
    """
    table_fields = list(df.columns)
    if_exists: str = 'append'

    if table_name in get_table_names(db_name):
        existing_fields: list[str] = get_table_fields(db_name, table_name)
        
        if existing_fields:
            assert all(t in existing_fields for t in table_fields)

        if replace: 
            # Replace the existing table
            if_exists = 'replace'

    df.to_sql(
        table_name, 
        _connect_to_any_db(db_name)[0],
        index = False,
        if_exists = if_exists,
        chunksize = chunksize,
    )

def correct_sql_statement(sql: str, keys: Iterable[str]) -> str:

    sql = sql.lower()
    for key in map(str.lower, keys):
        if key.isalpha(): continue
        
        padded_key = f"`{key}`"

        sql = sql.replace(padded_key, key) \
                    .replace(key, padded_key)
        
    return sql

def download_data_file(
    url: str,
    path_to_gzip: Path,
    path_to_data: Path,
) -> None:
    import requests
    import gzip

    from os import remove
    from shutil import copyfileobj

    print("Downloading data file:")
    print(f"> URL:  {url}")
    print(f"> GZIP: {str(path_to_gzip)}")
    print(f"> DATA: {str(path_to_data)}")

    if path_to_data.exists(): remove(path_to_data)
    if path_to_gzip.exists(): remove(path_to_gzip)

    with requests.get(url, stream=True) as r:
        print(f"> HTML: {r.status_code}")
        r.raise_for_status()

        with open(path_to_gzip, 'wb') as f_in:
            f_in.write(r.content)

        with gzip.open(path_to_gzip, 'rb') as f_in:
            with open(path_to_data, 'wb') as f_out:
                copyfileobj(f_in, f_out)

    remove(path_to_gzip)
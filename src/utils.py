"""
Utility functions for handling SQL databases.
"""
from pathlib import Path
from typing import Iterable
from sqlite3 import Connection

from pandas import DataFrame

_this_file: Path = Path(__file__)
PATH_TO_DB: Path = _this_file.parents[1] / 'data/db.db'

def connect_to_db() -> Connection:
    """
    Connects to the database.
    """
    from sqlite3 import connect
    return connect(PATH_TO_DB)

def get_table_names() -> list[str]:
    """
    Lists the names of all available tables in the database.
    """
    connection = connect_to_db()
    cursor = connection.cursor()
    return [
        table_info[1] \
        for table_info in cursor.execute("PRAGMA table_list").fetchall() \
        if not table_info[1].startswith('sqlite_')
    ]

def get_table_fields(table_name: str) -> list[str]:
    """
    Lists the available fields for the given table.
    """
    assert table_name in get_table_names()

    connection = connect_to_db()
    cursor = connection.cursor()
    return [
        table_info[1] \
        for table_info in cursor.execute(f"PRAGMA table_info(name)").fetchall() \
    ]

def remove_table(table_name: str) -> None:
    """
    Removes the designated table from the database.
    """
    assert table_name in get_table_names()

    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE {table_name}")

def add_table_to_db(
    table_name: str,
    table_fields: Iterable[str],
) -> Connection:
    """
    Creates a table with the given name and fields.
    """
    assert table_name not in get_table_names()

    sql: str = f"CREATE TABLE {table_name}(" + ", ".join(table_fields) + ")" 
    
    connection = connect_to_db()
    cursor = connection.cursor()
    cursor.execute(sql)

def write_to_db(
    table_name: str,
    df: DataFrame,
    replace: bool = True,
) -> None:
    """
    Writes a Pandas dataframe to the selected table.
    """
    table_fields = list(df.columns)
    if_exists: str = 'append'

    if table_name in get_table_names():
        existing_fields: list[str] = get_table_fields(table_name)
        
        if existing_fields:
            assert all(t in existing_fields for t in table_fields)

        if replace: 
            # Replace the existing table
            if_exists = 'replace'

    df.to_sql(
        table_name, 
        connect_to_db(),
        index = False,
        if_exists = if_exists,
    )
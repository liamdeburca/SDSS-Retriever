"""
SQLQuery class for constructing SQL queries easily.
"""
import sys
from pathlib import Path
from typing import Optional, Union, Iterable, Literal
from pandas import DataFrame

_this_file: Path = Path(__file__)
if (pkg_path := _this_file.parents[1]) not in sys.path:
    sys.path.append(pkg_path)

def _pad(s: str) -> str:
    return s if s.isalnum() else f"`{s}`"

def _rm_pad(s: str) -> str:
    return s.removeprefix('`').removesuffix('`')

class SQLQuery:
    """
    Light-weight SQL query builder. 
    """
    def __init__(self):
        from sqlite3 import Connection, Cursor
        
        self.connection: Optional[Connection] = None
        self.cursor: Optional[Cursor] = None

        self.table: Optional[str] = None
        self._columns: list[str] = []

        self.where_logic: list[str] = []
        self.order_by_logic: list[tuple[str, str]] = []
        self.limit: Optional[int] = None

    def __enter__(self) -> 'SQLQuery':
        return self
    
    def __exit__(self, type, value, traceback) -> None:
        self.cursor.close()
        self.cursor = None

        self.connection.close()
        self.connection = None

    def _build_query(self) -> tuple[list[str], str]:
        """
        Builds the SQL query.
        """
        query_elems: list[str] = []

        assert len(self._columns) > 0
        query_elems.append("SELECT " + ", ".join(self.columns))

        assert self.table is not None
        query_elems.append(f"FROM {self.table}")

        if self.where_logic:
            # Apply WHERE
            query_elems.append(
                "WHERE " + " AND ".join(self.where_logic)
            )

        if self.order_by_logic:
            # Apply ORDER_BY
            f = lambda a: "{} {}".format(*a)
            query_elems.append(
                "ORDER BY " + ", ".join(map(f, self.order_by_logic))
            )
            pass

        if self.limit:
            # Apply LIMIT
            query_elems.append(f"LIMIT {self.limit}")

        query: str = ' '.join(query_elems) + ';'

        return query_elems, query
    
    def _query(
        self, 
        sql: str,
        out_type: Literal['dict', 'dataframe'] = 'dataframe',
    ) -> Union[dict, DataFrame]:
        """
        Uses a custom sql query to retrieve the data.
        """
        from pandas import read_sql_query

        if out_type == 'dict':
            self._query(sql, out_type='dataframe').to_dict(orient='list')

        return read_sql_query(sql, self.connection)
    
    @property
    def column_info(self):
        """
        Get basic table information.
        """
        return self.cursor.execute(
            "PRAGMA table_info({})".format(self.table)
        ).fetchall()


    @property
    def table_names(self) -> list[str]:
        assert self.connection is not None
        assert self.cursor is not None

        if getattr(self, '_table_names', None) is not None:
            return self._table_names

        self._table_names: list[str] = [
            a[1] for a in self.cursor.execute("PRAGMA table_list").fetchall()
        ]
        
        return self._table_names
    
    @property
    def column_names(self) -> list[str]:
        assert self.connection is not None
        assert self.cursor is not None
        assert getattr(self, 'table') is not None

        if getattr(self, '_column_names', None) is not None:
            return self._column_names

        self._column_names: list[str] = [
            cinfo[1] for cinfo in self.column_info
        ]

        return self._column_names
    
    @property
    def columns(self) -> list[str]:
        """
        List of formatted column names.
        """
        return list(map(_pad, self._columns))

    def connectToDatabase(self) -> 'SQLQuery':
        """
        Connects this instance to the designated (or default) database. 
        """
        from src.utils import connect_to_db
        self.connection = connect_to_db()
        self.cursor = self.connection.cursor()
        return self

    @staticmethod
    def START() -> 'SQLQuery':
        """
        Creates a Query instance and connects it to a database.
        """
        return SQLQuery().connectToDatabase()
    
    def STOP(
        self, 
        out_type: Literal['dict', 'dataframe'] = 'dataframe',
    ) -> Union[dict, DataFrame]:
        """
        Builds the SQL query and retrieves the data.
        """
        return self._query(self._build_query(out_type=out_type)[1])
    
    def FROM(
        self,
        table: str,
    ) -> 'SQLQuery':
        """
        Specifies the table from which data is queried from. This table must 
        exist. 
        """
        if table not in (table_names := self.table_names):
            raise ValueError(
                f"Table {table} was not found among {table_names}"
            )

        self.table = table

        return self
    
    def SELECT(
        self,
        *column: str,
    ) -> 'SQLQuery':
        """
        Specifies which value(s) to retrieve from the database table.
        """
        if len(column) > 1:
            assert '*' not in column
            _ = list(map(self.SELECT, column))
            return self
        
        column = column[0]
        
        if column == '*':
            self._columns.clear()
            self._columns.extend(self.column_names)
        else:
            if column not in self.column_names:
                raise KeyError(f"{column} is not a valid column name")
            
            self._columns.append(column)

        return self
    
    def WHERE(
        self,
        column: Union[str, Iterable[str]],
        logic: str,
    ) -> 'SQLQuery':
        """
        Applies the specified logic.
        """
        if isinstance(column, str):
            assert column in self.column_names
            cols = (_pad(column))
        else:
            assert all(c in self.column_names for c in column)
            cols = tuple(map(_pad, column))

        self.where_logic.append(logic.format(*cols))

        return self
    
    def ORDER_BY(
        self,
        column: Union[str, Iterable[str]],
        descending: Union[bool, Iterable[bool]] = False,
    ) -> 'SQLQuery':
        """
        Sorts the returned data according to the specified column.
        """
        if isinstance(column, str):
            assert column in self.column_names
            assert isinstance(descending, bool)

            cols: list[str] = [column]
            desc: list[bool] = [descending]
        else:
            assert all(c in self.column_names for c in column)

            cols: list[str] = list(column)
            if isinstance(descending, bool):
                desc: list[bool] = len(column) * [descending]
            else:
                desc: list[bool] = list(descending)
                assert len(cols) == len(desc)

        cols = list(map(_pad, cols))

        self.order_by_logic.extend(
            (c, 'DESC' if d else 'ASC') \
            for c, d in zip(cols, desc)
        )

        return self
    
    def LIMIT(
        self,
        limit: int,
    ) -> 'SQLQuery':
        """
        Limits the size (no. of rows) of the result.
        """
        assert limit >= 1

        self.limit: int = limit

        return self    
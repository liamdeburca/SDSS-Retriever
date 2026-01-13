"""
This script includes:

1.  Creating of a new SQL table with the selected quasar samples.
2.  Downloading of spectra using multiprocessing.

"""
import sys
from pathlib import Path
from argparse import Namespace
from pandas import DataFrame
from multiprocessing.pool import Pool

_this_file: Path = Path(__file__)
if (pkg_path := _this_file.parents[1]) not in sys.path:
    sys.path.append(str(pkg_path))

from src.sqlquery import SQLQuery
from src.utils import write_to_db
from src.sdss_retriever import SDSSRetriever
from src.shen_2011.sdssparser import SDSSParser

def main(namespace: Namespace, pool: Pool) -> None:
    # Retrieve samples using SQL query

    sql: str = SDSSParser.correctSQLStatement(namespace.s)
    table_name: str = namespace.t

    print("Downloading spectra:", end='\n')
    print(f"> SQL query:        {sql}")
    print(f"> Output table:     {table_name}")
    print(f"> No. of processes: {pool._processes}", end='\n')

    with SQLQuery.START() as q:
        df: DataFrame = q._query(sql)

    print(f"> Retrieved DataFrame w/ shape: {df.shape}")

    # Write table with selected samples
    write_to_db('samples', table_name, df, replace=True)
    print(f"> Wrote DataFrame to SQL table.")

    # Download spectra using multiprocessing

    download_itr = SDSSRetriever(pool)(df)
    print(list(map(bool, download_itr)))

    return 
        
if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(
        description = "Download SDSS spectra for selected quasar samples."
    )
    parser.add_argument(
        '-s',
        type = str,
        required = True,
        default = "SELECT * FROM shen_2011 LIMIT 10",
        help = "SQL statement to select quasar samples.",
    )
    parser.add_argument(
        "-t",
        type = str,
        required = True,
        help = "Name of the output SQL table to store selected samples.",
    )
    args = parser.parse_args()

    with Pool() as pool:
        main(args, pool)
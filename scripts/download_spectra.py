"""
This script includes:

1.  Creating of a new SQL table with the selected quasar samples.
2.  Downloading of spectra using multiprocessing.

"""
import sys
from pathlib import Path
from argparse import Namespace
from pandas import DataFrame
from numpy import mean

_this_file: Path = Path(__file__)
if (pkg_path := _this_file.parents[1]) not in sys.path:
    sys.path.append(str(pkg_path))

from src.sqlquery import SQLQuery
from src.utils import write_to_db, correct_sql_statement
from src.retriever import AsyncSDSSRetriever

from src.parsers import PARSERS

def main(namespace: Namespace) -> None:
    # Retrieve samples using SQL query    

    print("Downloading spectra:", end='\n')
    print(f"> SQL query:        {namespace.s}")
    print(f"> Output table:     {namespace.t}")
    print(f"> Timeout:          {namespace.timeout} seconds")
    print(f"> Batch size:       {namespace.bsize}")

    parser_cls = PARSERS[namespace.db]
    retriever = AsyncSDSSRetriever(
        timeout = namespace.timeout,
        data_release = namespace.dr,
        batch_size = namespace.bsize,
    )

    sql: str = correct_sql_statement(
        namespace.s, parser_cls.keys(),
    ).format(namespace.db)

    with SQLQuery.START() as q:
        df: DataFrame = q._query(sql)

    print(f"> Retrieved DataFrame w/ shape: {df.shape}")

    # Write table with selected samples
    write_to_db('samples', namespace.t, df, replace=True)
    print(f"> Wrote DataFrame to SQL table.")

    # Download spectra
    print("> Downloading spectra...")
    res = retriever(df)
    successes = list(map(bool, res))

    print(f"Finished downloading spectra: {mean(successes)*100:.2f}% success.")
        
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
    parser.add_argument(
        '--db',
        type = str,
        required = False,
        default = 'shen_2011',
        choices = tuple(PARSERS.keys()),
    )
    parser.add_argument(
        '--timeout',
        type = float,
        required = False,
        default = 60.0,
        help = "Maximum timeout (in seconds) for each spectrum retrieval.", 
    )
    parser.add_argument(
        '--dr',
        type = int,
        required = False,
        default = 17,
        help = "SDSS Data Release number.",
    )
    parser.add_argument(
        '--bsize',
        type = int,
        required = False,
        default = 100,
        help = "Batch size for asynchronous retrieval.",
    )

    main(parser.parse_args())
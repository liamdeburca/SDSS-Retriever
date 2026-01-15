"""
Creates an SQL database using a given pre-parsed .dat file.
"""
import sys

from pathlib import Path
from argparse import Namespace

DEFAULT_NAMESPACE: Namespace = Namespace()
DEFAULT_NAMESPACE.name = 'shen_2011'
DEFAULT_NAMESPACE.replace = True

_this_file: Path = Path(__file__)
if (pkg_path := str(_this_file.parents[1])) not in sys.path:
    sys.path.append(pkg_path)

from src.parsers import PARSERS
from src.parsers.parser import SDSSParser
from src.utils import write_to_db

def main(namespace: Namespace) -> None:
    from pandas import DataFrame

    print("Initialising database:")
    print(f"> Parsed: {namespace}")

    parser_cls = PARSERS[namespace.name]
    path_to_data: Path = parser_cls.path_to_data()

    if path_to_data.exists():
        print(f"> Found data for {namespace.name}!")
    else:
        print(f"> Could not find data for {namespace.name}!")
        return

    parser: SDSSParser = PARSERS[namespace.name](path_to_data)
    # Create DataFrame
    df: DataFrame = parser.toDataFrame(with_duplicates=False)

    # Write or append to dataframe
    write_to_db('db', namespace.name, df, replace=namespace.replace)

if __name__ == '__main__':
    from argparse import ArgumentParser
    
    parser = ArgumentParser(
        'init_db',
        description = \
            'creates an SQLite table using a given pre-parsed .dat file'
    )
    parser.add_argument(
        '--name',
        required = False,
        default = 'shen_2011',
        choices = tuple(PARSERS.keys()),
        help = 'desired table name',
    )
    parser.add_argument(
        '--replace',
        required = False,
        default = True,
        type = bool,
        help = 'whether to replace any existing table with the same name'
    )

    main(parser.parse_args())
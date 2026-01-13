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
if (pkg_path := _this_file.parents[1]) not in sys.path:
    sys.path.append(str(pkg_path))

from src.shen_2011.sdssparser import SDSSParser as shen_2011_parser
AVAILABLE_PARSERS: dict = {
    'shen_2011': shen_2011_parser,
}

from src.utils import write_to_db

def main(namespace: Namespace) -> None:
    from pandas import DataFrame

    print("Initialising database:")
    print(f"> Parsed: {namespace}")

    path_to_data: Path = \
        _this_file.parents[1] / f"data/{namespace.name}/catalogue.dat"
    
    if path_to_data.exists():
        print(f"> Found data for {namespace.name}!")
    else:
        print(f"> Could not find data for {namespace.name}!")
        return
    
    parser = AVAILABLE_PARSERS[namespace.name](path_to_data)

    # Create DataFrame
    df: DataFrame = parser.to_dataframe(with_duplicates=False)

    # Write or append to dataframe
    write_to_db(namespace.name, df, replace=namespace.replace)

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
        choices = ('shen_2011',),
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
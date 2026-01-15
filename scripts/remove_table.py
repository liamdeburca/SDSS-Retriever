"""
This script locates and removes a specific table from a database.
"""
import sys
from pathlib import Path
from argparse import Namespace

_this_file: Path = Path(__file__)
if (pkg_path := str(_this_file.parents[1])) not in sys.path:
    sys.path.append(pkg_path)

from src import utils

def main(args: Namespace) -> None:

    paths: dict[str, Path] = {
        'db': utils.PATH_TO_DB,
        'samples': utils.PATH_TO_SAMPLES,
    }

    path: Path = paths[args.db]
    if not path.exists():
        raise FileNotFoundError(
            f"Database at {str(path)} was not found!"
        )

    table_names = utils.get_table_names(args.db)
    if not args.name in table_names:
        raise ValueError(
            f"Table {args.name} is not one of {tuple(table_names)}!"
        )

    utils.remove_table(args.db, args.name)

if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(
        description = "Remove the specified table from the database"
    )
    parser.add_argument(
        '--db',
        type = str,
        required = True,
        options = ('db', 'samples'),
    )
    parser.add_argument(
        '--name',
        type = str,
        required = True,
    )
    main(parser.parse_args())
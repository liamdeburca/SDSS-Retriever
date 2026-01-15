"""
This script locates and describes the 'db' and 'samples' SQLite databases.
"""
if __name__ == '__main__':
    import sys
    from pathlib import Path

    _this_file: Path = Path(__file__)
    if (pkg_path := str(_this_file.parents[1])) not in sys.path:
        sys.path.append(pkg_path)

    from src import utils

    if (path := utils.PATH_TO_DB).exists():
        print(f"Found main DB ('db.db') in: {str(path.parent)}")

        table_names: list[str] = utils.get_table_names('db')
        n_tables: int = len(table_names)
        
        if n_tables == 0:
            print("> DB contains no fields!")
        else:
            for count, name in enumerate(table_names, start=1):
                print(f"> [{count}/{n_tables}] {name}")

    if (path := utils.PATH_TO_SAMPLES).exists():
        print(f"Found secondary DB ('samples.db') in: {str(path.parent)}")

        table_names: list[str] = utils.get_table_names('samples')
        n_tables: int = len(table_names)
        
        if n_tables == 0:
            print("> DB contains no fields!")
        else:
            for count, name in enumerate(table_names, start=1):
                print(f"> [{count}/{n_tables}] {name}")
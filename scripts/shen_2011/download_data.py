"""
This script downloads, unpacks, and formats data from Shen et al. (2011).
"""
from pathlib import Path

_this_file: Path = Path(__file__)
PATH_TO_DIR: Path = _this_file.parents[2] / 'data/shen_2011'

URL: str = 'https://cdsarc.cds.unistra.fr/ftp/J/ApJS/194/45/catalog.dat.gz'
PATH_TO_GZIP: Path = PATH_TO_DIR / 'catalogue.dat.gz'
PATH_TO_DATA: Path = PATH_TO_DIR / 'catalogue.dat'

if __name__ == '__main__':
    import sys

    if (pkg_path := str(_this_file.parents[2])) not in sys.path:
        sys.path.append(pkg_path)

    from src.utils import download_data_file

    PATH_TO_DIR.mkdir(parents=True, exist_ok=True)

    download_data_file(
        URL,
        PATH_TO_GZIP,
        PATH_TO_DATA,
    )

    sys.path.remove(pkg_path)
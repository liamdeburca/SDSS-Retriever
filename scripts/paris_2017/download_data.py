"""
This script downloads, unpacks, and formats data from Paris et al. (2017).
"""
from pathlib import Path

_this_file: Path = Path(__file__)
PATH_TO_DIR: Path = _this_file.parents[2] / 'data/paris_2017'

URL: str = 'https://cdsarc.cds.unistra.fr/ftp/VII/279/dr12q.dat.gz'
PATH_TO_GZIP: Path = PATH_TO_DIR / 'catalogue.dat.gz'
PATH_TO_DATA: Path = PATH_TO_DIR / 'catalogue.dat'

URL_BAL: str = 'https://cdsarc.cds.unistra.fr/ftp/VII/279/dr12qbal.dat.gz'
PATH_TO_GZIP_BAL: Path = PATH_TO_DIR / 'catalogue_bal.dat.gz'
PATH_TO_DATA_BAL: Path = PATH_TO_DIR / 'catalogue_bal.dat'

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
    download_data_file(
        URL_BAL,
        PATH_TO_GZIP_BAL,
        PATH_TO_DATA_BAL,
    )

    sys.path.remove(pkg_path)
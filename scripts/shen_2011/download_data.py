"""
This script downloads, unpacks, and formats data from Shen et al. (2011).
"""
from pathlib import Path

CHUNK_SIZE: int = 8192
URL: str = 'https://cdsarc.cds.unistra.fr/ftp/J/ApJS/194/45/catalog.dat.gz'

_this_file: Path = Path(__file__)
PATH_TO_GZIP: Path = _this_file.parents[2] / 'data/shen_2011/catalogue.dat.gz'
PATH_TO_DATA: Path = _this_file.parents[2] / 'data/shen_2011/catalogue.dat'

def get_description(
    n_chunks: int, 
    chunk_size: int = CHUNK_SIZE,
) -> str:
    
    if (n_bytes := n_chunks * chunk_size) > 1e9:
        unit = 'B'
        val = n_bytes / 1e9
    elif n_bytes > 1e6:
        unit = 'M'
        val = n_bytes / 1e6
    elif n_bytes > 1e3:
        unit = 'K'
        val = n_bytes / 1e3
    else:
        unit = ''
        val = n_bytes

    return f"{val:.1f}{unit} bytes written"

def main() -> None:
    import requests
    import gzip

    from os import remove
    from tqdm import tqdm
    from shutil import copyfileobj

    if PATH_TO_GZIP.exists(): remove(PATH_TO_GZIP)
    if PATH_TO_DATA.exists(): remove(PATH_TO_DATA)

    with requests.get(URL, stream=True) as r:
        r.raise_for_status()

        with open(PATH_TO_GZIP, 'wb') as f_in:
            pbar = tqdm(
                enumerate(r.iter_content(chunk_size=CHUNK_SIZE), start=1),
                leave = True,
            )
            pbar.set_description(get_description(0))
            for n_chunks, chunk in pbar:
                f_in.write(chunk)
                pbar.set_description(get_description(n_chunks))

        with gzip.open(PATH_TO_GZIP, 'rb') as f_in:
            with open(PATH_TO_DATA, 'wb') as f_out:
                copyfileobj(f_in, f_out)

    remove(PATH_TO_GZIP)

if __name__ == '__main__':
    if not (parent := PATH_TO_GZIP.parent).exists(): parent.mkdir()
    main()
from abc import ABC
from typing import Iterator, Union, Iterator, Optional, Generator
from pathlib import Path
from functools import lru_cache
import h5py
from astropy.io.fits import HDUList
from numpy import ndarray, array

_path_to_cache: Path = Path(__file__).parents[2] / ".cache"

class AbstractRetriever(ABC):
    """Abstract base class for SDSS spectrum retrievers."""

    def __init__(
        self,
        timeout: float = 60.0,
        data_release: Optional[int] = None,
        show_progress: bool = True,
    ):
        self.timeout: float = timeout
        self.data_release: Optional[int] = data_release
        self.show_progress: bool = show_progress

    def _format_args_into_iterables(
        self,
        *args,
    ) -> tuple[Iterator[int], Iterator[int], Iterator[int], int]:
        from pandas import DataFrame
        from itertools import repeat

        if len(args) == 1:
            if isinstance(data := args[0], dict):
                plate: Union[int, list[int]] = data['plate']
                fiber: Union[int, list[int]] = data['fiber']
                mjd:   Union[int, list[int]] = data['mjd']
            elif isinstance(data, DataFrame):
                plate: list[int] = data['plate'].to_list()
                fiber: list[int] = data['fiber'].to_list()
                mjd:   list[int] = data['mjd'].to_list()
            else:
                raise ValueError

            return self._format_args_into_iterables(plate, fiber, mjd)

        elif len(args) == 3:
            plate: Union[int, list[int]] = args[0]
            fiber: Union[int, list[int]] = args[1]
            mjd:   Union[int, list[int]] = args[2]

        else:
            raise ValueError

        if isinstance(plate, int): plate = (plate,)
        if isinstance(fiber, int): fiber = (fiber,)
        if isinstance(mjd, int):   mjd   = (mjd,)

        n_max: int = max(map(len, (plate, fiber, mjd)))
        assert all(len(a) in (1, n_max) for a in (plate, fiber, mjd))

        f = lambda v: v if (len(v) == n_max) else repeat(v[0], n_max)
        plate_itr: Iterator[int] = f(plate)
        fiber_itr: Iterator[int] = f(fiber)
        mjd_itr:   Iterator[int] = f(mjd)

        return plate_itr, fiber_itr, mjd_itr, n_max
    
    @staticmethod
    def _translate_hdul(hdul: HDUList) -> tuple[ndarray[float]]:
        """
        Translates a HDU-list to a tuple of numpy arrays. 
        """
        from numpy import where, nan
        from astropy.io.fits.fitsrec import FITS_rec        
        data: FITS_rec = hdul[1].data
        return (
            10**data.loglam,
            data.flux,
            where(data.ivar > 0, data.ivar, nan)**-0.5,
        )
    
    @lru_cache(maxsize=128)
    def _get_url_to_spectrum(
        self,
        plate: int,
        fiber: int,
        mjd: int,
    ) -> str:
        """
        Note
        ----
        This method is based heavily on the source code of 'get_spectra_async'
        from v. 0.4.11 of AstroQuery. If AstroQuery is updated, this method may
        not work as intended, so be sure to check for any changes.
        """
        import astroquery
        assert astroquery.__version__ == '0.4.11'

        from numpy import integer
        from astroquery.sdss import SDSS, conf

        kwargs = dict(
            plate = plate,
            fiberID = fiber,
            mjd = mjd,
            data_release = self.data_release or astroquery.sdss.conf.default_release,
            timeout = self.timeout,
        )

        try:                   res = SDSS.query_specobj(**kwargs)
        except Exception as e: res = None
        
        if (res is None) or len(res) == 0: return None
        
        row = res[0]

        linkstr = SDSS.SPECTRA_URL_SUFFIX
        # _parse_result returns bytes (requiring a decode) for
        # - instruments
        # - run2d sometimes (#739)
        if isinstance(row['run2d'], bytes):
            run2d = row['run2d'].decode()
        elif isinstance(row['run2d'], (integer, int)):
            run2d = str(row['run2d'])
        else:
            run2d = row['run2d']
        format_args = dict()
        format_args['base'] = conf.sas_baseurl
        format_args['dr'] = self.data_release
        format_args['redux_path'] = 'sdss/spectro/redux'
        format_args['run2d'] = run2d
        format_args['spectra_path'] = 'spectra'
        format_args['mjd'] = row['mjd']
        try:
            format_args['plate'] = row['plate']
            format_args['fiber'] = row['fiberID']
        except KeyError:
            format_args['fieldid'] = row['fieldID']
            format_args['catalogid'] = row['catalogID']
        if self.data_release > 15 and run2d not in ('26', '103', '104'):
            #
            # Still want this applied to data_release > 17.
            #
            format_args['spectra_path'] = 'spectra/full'
        if self.data_release > 17:
            #
            # This change will fix everything except run2d==v6_0_4 in DR18,
            # which is handled by the if major > 5 block below.
            #
            format_args['redux_path'] = 'spectro/sdss/redux'
            match_run2d = SDSS.PARSE_BOSS_RUN2D.match(run2d)
            if (match_run2d is not None) \
                and (int(match_run2d.group('major')) > 5):
                linkstr = linkstr \
                    .replace(
                        '/{plate:0>4d}/', 
                        '/{fieldid:0>4d}p/{mjd:5d}/',
                    ) \
                    .replace(
                        'spec-{plate:0>4d}-{mjd}-{fiber:04d}.fits',
                        'spec-{fieldid:0>4d}-{mjd:5d}-{catalogid:0>11d}.fits'
                    )

        return linkstr.format(**format_args)
    
    @lru_cache(maxsize=128)
    def _create_cache_hash(
        self,
        plate: int,
        fiber: int,
        mjd: int,
    ) -> Optional[str]:
        from hashlib import md5

        url = self._get_url_to_spectrum(plate, fiber, mjd)
        if url is None: return None

        return md5(url.encode()).hexdigest()

    def _get_from_cache(
        self,
        plate: int,
        fiber: int,
        mjd: int,
    ) -> Union[tuple[ndarray, ndarray, ndarray], None]:
        hash_str: str = self._create_cache_hash(plate, fiber, mjd)
        cache_file: Path = _path_to_cache / f"{hash_str}.h5"

        if not cache_file.exists(): return None

        with h5py.File(cache_file, 'r') as f:
            return (array(f['x']), array(f['y']), array(f['dy']))
            
    def _add_to_cache(
        self,
        plate: int,
        fiber: int,
        mjd: int,
        data: tuple[ndarray, ndarray, ndarray],
    ) -> None:
        hash_str: str = self._create_cache_hash(plate, fiber, mjd)
        cache_file: Path = _path_to_cache / f"{hash_str}.h5"

        if not _path_to_cache.exists():
            _path_to_cache.mkdir(parents=True, exist_ok=True)

        if cache_file.exists(): return

        with h5py.File(cache_file, 'w') as f:
            f.create_dataset('x',  data=data[0], compression='gzip', compression_opts=9)
            f.create_dataset('y',  data=data[1], compression='gzip', compression_opts=9)
            f.create_dataset('dy', data=data[2], compression='gzip', compression_opts=9)

    def _remove_from_cache(
        self,
        plate: int,
        fiber: int,
        mjd: int,
    ) -> None:
        from os import remove

        hash_str: str = self._create_cache_hash(plate, fiber, mjd)
        cache_file: Path = _path_to_cache / f"{hash_str}.h5"

        assert cache_file.exists()

        remove(cache_file)

    def _clear_cache(self) -> None:
        from os import remove
        _ = list(map(remove, _path_to_cache.glob('*.h5')))
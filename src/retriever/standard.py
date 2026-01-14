"""
Class for retrieving SDSS spectra using AstroQuery's 'get_spectra'.
"""
from typing import Optional, Union, Iterator, Iterable
from astropy.io.fits import HDUList
from numpy import ndarray
from multiprocessing.pool import Pool

from .abstract_retriever import AbstractRetriever

class SDSSRetriever(AbstractRetriever):
    def __init__(
        self, 
        timeout: float = 60.0,
        data_release: int = 17,
        show_progress: bool = True,
        pool: Optional[Pool] = None,
    ):
        super().__init__(
            timeout = timeout,
            data_release = data_release,
            show_progress = show_progress,
        )
        self.pool: Optional[Pool] = pool
        
    def fetch_spectrum(
        self,
        args: tuple[int, int, int]
    ) -> Optional[tuple[ndarray[float]]]:
        import warnings
        from astropy.utils.exceptions import AstropyWarning
        from astroquery.sdss import SDSS
    
        plate: int = args[0]
        fiber: int = args[1]
        mjd:   int = args[2]

        cache_result = self._get_from_cache(plate, fiber, mjd)
        if cache_result is not None:
            return cache_result
        
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', AstropyWarning)
                                  
            try:
                res = SDSS.get_spectra(
                    plate = plate,
                    fiberID = fiber,
                    mjd = mjd,
                    timeout = self.timeout,
                    data_release = self.data_release,
                    show_progress = False,
                    cache = False, # Use custom caching
                )
            except:
                return None

            if (res is not None) and len(res) > 0: 
                data = self._translate_hdul(res[0])
                self._add_to_cache(plate, fiber, mjd, data)
                return data

            return None

    def get_spectrum(self, *args) -> Iterator[tuple[ndarray, ndarray, ndarray]]:
        from tqdm import tqdm

        loop = self._format_args_into_iterables(*args)
        _zip = zip(*loop[:3])
        func = self.fetch_spectrum

        if self.pool is not None:
            return (
                tqdm(self.pool.imap(func, _zip), total=loop[-1]) \
                if self.show_progress \
                else self.pool.map(func, _zip)
            )
        else:
            return (
                tqdm(map(func, _zip), total=loop[-1]) \
                if self.show_progress \
                else map(func, _zip) 
            )
        
    def __call__(self, *args) -> list[tuple[ndarray, ndarray, ndarray]]:
        assert len(args) >= 1
        return list(self.get_spectrum(*args))
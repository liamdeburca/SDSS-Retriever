"""
Class for retrieving SDSS spectra using AstroQuery's 'get_spectra'.
"""
from typing import Optional, Union, Iterable, Iterator
from astropy.io.fits import HDUList
from numpy import ndarray
from multiprocessing.pool import Pool

class SDSSRetriever:
    def __init__(self, pool: Optional[Pool] = None):
        self.pool: Optional[Pool] = pool

    def __call__(
        self,
        plate: Union[int, Iterable[int]],
        fiber: Union[int, Iterable[int]],
        mjd: Union[int, Iterable[int]],
        *,
        timeout: float = 60.0,
        data_release: int = 17,
        show_progress: bool = True,
    ) -> Iterator:
        from itertools import repeat
        from tqdm import tqdm

        if isinstance(plate, int): plate = (plate,)
        if isinstance(fiber, int): fiber = (fiber,)
        if isinstance(mjd, int):   mjd   = (mjd,)

        n_max: int = max(map(len, (plate, fiber, mjd)))
        assert all(len(a) in (1, n_max) for a in (plate, fiber, mjd))

        _zip = zip(
            plate if (len(plate) == n_max) else repeat(plate[0], n_max),
            fiber if (len(fiber) == n_max) else repeat(fiber[0], n_max),
            mjd   if (len(mjd) == n_max)   else repeat(mjd[0],   n_max),
            repeat(timeout, n_max),
            repeat(data_release, n_max),
        )

        if self.pool is not None:
            return (
                tqdm(self.pool.imap(SDSSRetriever._query, _zip), total=n_max) \
                if show_progress \
                else self.pool.map(SDSSRetriever._query, _zip)
            )
        else:
            return (
                tqdm(map(SDSSRetriever._query, _zip), total=n_max) \
                if show_progress \
                else map(SDSSRetriever._query, _zip) 
            )

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
        
    @staticmethod
    def _query(
        args: tuple[int, int, int, float, int]
    ) -> Optional[tuple[ndarray[float]]]:
        import warnings
        from astropy.utils.exceptions import AstropyWarning
        from astroquery.sdss import SDSS
    
        plate: int = args[0]
        fiber: int = args[1]
        mjd: int = args[2]
        timeout: float = args[3]
        data_release: int = args[4]

        with warnings.catch_warnings():
            warnings.simplefilter('ignore', AstropyWarning)

            try:
                res = SDSS.get_spectra(
                    plate = plate,
                    fiberID = fiber,
                    mjd = mjd,
                    timeout = timeout,
                    data_release = data_release,
                    show_progress = False,
                )
            except:
                return None

            if (res is not None) and len(res) > 0: 
                return SDSSRetriever._translate_hdul(res[0])

            return None        
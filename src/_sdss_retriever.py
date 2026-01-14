"""
Asynchronous SDSS spectrum retrieval using asyncio and aiohttp.
This can be significantly faster than multiprocessing for I/O-bound downloads.
"""
# import asyncio
from aiohttp import ClientSession
from asyncio import Semaphore
from typing import Optional, Union, Iterable, List, Tuple
from numpy import ndarray
from astropy.io import fits

_base_url: str  = "https://dr{}.sdss.org/sas/dr{}/sdss/spectro/redux"
_spec_file: str = "spec-{:04d}-{:05d}-{:04d}.fits"
_url = "{}/{}/{:04d}/{}"

class AsyncSDSSRetriever:
    """
    Async retriever for SDSS spectra.
    
    Can handle hundreds of concurrent downloads, much faster than
    multiprocessing for I/O-bound tasks.
    """
    
    def __init__(
        self, 
        max_concurrent: int = 50, 
        timeout: float = 60.0,
    ):
        """
        Initialize async retriever.
        
        Parameters:
        -----------
        max_concurrent : int, optional
            Maximum number of concurrent requests (default: 50)
            Higher values = faster but more load on server
        timeout : float, optional
            Timeout per request in seconds (default: 60.0)
        """
        self.max_concurrent: int = max_concurrent
        self.timeout:        float = timeout

    @property
    def semaphore(self) -> Semaphore:
        from asyncio import Semaphore
        return Semaphore(self.max_concurrent)
    
    async def _fetch_spectrum(
        self,
        session: ClientSession,
        plate: int,
        fiber: int,
        mjd: int,
        data_release: int,
    ) -> Optional[Tuple[ndarray, ndarray, ndarray]]:
        """
        Fetch a single spectrum asynchronously.
        
        Returns:
        --------
        tuple or None
            (wavelength, flux, error) if successful, None otherwise
        """
        from asyncio import TimeoutError
        from io import BytesIO

        # Use semaphore to limit concurrent requests
        async with self.semaphore:
            # Construct SDSS URL
            run2d = "26" if data_release <= 7 else "v5_13_2"

            base_url  = _base_url .format(data_release, data_release)
            spec_file = _spec_file.format(plate, mjd, fiber)
            url       = _url      .format(base_url, run2d, plate, spec_file)
            
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        # Read FITS data
                        content = await response.read()
                        hdul = fits.open(BytesIO(content))
                        
                        # Extract spectrum data
                        data = hdul[1].data
                        x  = 10**data['loglam']
                        y  = data['flux']
                        dy = data['ivar']**-0.5
                        
                        hdul.close()

                        return x, y, dy
                    else:
                        return None
                        
            except TimeoutError: return None
            except Exception: return None
    
    async def get_spectrum(
        self,
        plate: Union[int, Iterable[int]],
        fiber: Union[int, Iterable[int]],
        mjd: Union[int, Iterable[int]],
        data_release: int = 17,
        show_progress: bool = True,
    ) -> List[Optional[Tuple[ndarray, ndarray, ndarray]]]:
        """
        Download multiple spectra asynchronously.
        
        Parameters:
        -----------
        plates : int or iterable of int
            Plate number(s)
        fibers : int or iterable of int
            Fiber number(s)
        mjds : int or iterable of int
            MJD value(s)
        data_release : int, optional
            SDSS data release (default: 17)
        show_progress : bool, optional
            Show progress bar (default: True)
        
        Returns:
        --------
        list
            List of (wavelength, flux, error) tuples or None for failures
        
        Examples:
        ---------
        >>> retriever = AsyncSDSSRetriever(max_concurrent=50)
        >>> plates = [751, 752, 753]
        >>> fibers = [280, 150, 420]
        >>> mjds = [52251, 52252, 52253]
        >>> spectra = await retriever.get_spectrum(plates, fibers, mjds)
        """
        from asyncio import gather
        from aiohttp import ClientSession, TCPConnector, ClientTimeout
        from tqdm.asyncio import tqdm_asyncio
        from itertools import repeat
        
        # Convert to iterables
        if isinstance(plate, int): plate = (plate,)
        if isinstance(fiber, int): fiber = (fiber,)
        if isinstance(mjd, int):   mjd   = (mjd,)
        
        # Validate lengths
        n_max = max(map(len, (plate, fiber, mjd)))
        assert all(len(a) in (1, n_max) for a in (plate, fiber, mjd))
        
        # Expand to full length
        plate = plate if len(plate) == n_max else repeat(plate[0], n_max)
        fiber = fiber if len(fiber) == n_max else repeat(fiber[0], n_max)
        mjd   = mjd   if len(mjd) == n_max   else repeat(mjd[0],   n_max)
        
        # Create session with timeout
        timeout:   ClientTimeout = ClientTimeout(total=self.timeout)
        connector: TCPConnector  = TCPConnector(limit=self.max_concurrent)
        
        async with ClientSession(timeout=timeout, connector=connector) as session:
            # Create tasks for all downloads
            tasks = [
                self._fetch_spectrum(session, *args, data_release)
                for args in zip(plate, fiber, mjd)
            ]
            
            # Execute with optional progress bar
            results = await (
                 tqdm_asyncio.gather(*tasks) \
                 if show_progress \
                 else gather(*tasks)
            )
        
        return results
    
    def __call__(
        self,
        plate: Union[int, Iterable[int]],
        fiber: Union[int, Iterable[int]],
        mjd: Union[int, Iterable[int]],
        data_release: int = 17,
        show_progress: bool = True,
    ):
        """
        Synchronous wrapper for get_spectrum.
        Runs the async function in an event loop.
        """
        from asyncio import run
        return run(self.get_spectrum(
            plate, fiber, mjd, 
            data_release=data_release, show_progress=show_progress,
        ))
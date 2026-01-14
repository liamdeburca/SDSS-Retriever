from asyncio import Semaphore
from aiohttp import ClientSession
from typing import Optional, Generator
from numpy import ndarray

from .abstract_retriever import AbstractRetriever

class AsyncSDSSRetriever(AbstractRetriever):
    def __init__(
        self, 
        max_concurrent: int = 50,
        batch_size: int = 100,
        timeout: float = 60.0,
        data_release: int = 17,
        show_progress: bool = True,
    ):
        super().__init__(
            timeout = timeout,
            data_release = data_release,
            show_progress = show_progress,
        )
        self.max_concurrent: int = max_concurrent
        self.batch_size:     int = batch_size

    @property
    def semaphore(self) -> Semaphore:
        from asyncio import Semaphore
        return Semaphore(self.max_concurrent) 
    
    async def fetch_spectrum(
        self,
        session: ClientSession,
        args: tuple[int, int, int],
    ) -> Optional[tuple[ndarray, ndarray, ndarray]]:
        """
        Fetch a single spectrum asynchronously.
        
        Returns:
        --------
        tuple or None
            (wavelength, flux, error) if successful, None otherwise
        """
        from asyncio import TimeoutError
        from io import BytesIO
        from astropy.io import fits

        plate, fiber, mjd = args

        cache_result = self._get_from_cache(plate, fiber, mjd)
        if cache_result is not None:
            return cache_result
        
        url: str = self._get_url_to_spectrum(plate, fiber, mjd)
        if url is None: return None

        # Use semaphore to limit concurrent requests
        async with self.semaphore:
            # Construct SDSS URL
            
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    # Read FITS data
                    content = await response.read()

                    with fits.open(BytesIO(content)) as hdul:
                        data = self._translate_hdul(hdul)
                        self._add_to_cache(plate, fiber, mjd, data)
                        return data
                        
            except TimeoutError: return None
            except Exception: return None

    async def get_spectrum(
        self, 
        *args, 
        leave_pbar: bool = True,
    ) -> list[Optional[tuple[ndarray, ndarray, ndarray]]]:
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
        
        Returns:
        --------
        list
            List of (wavelength, flux, error) tuples or None for failures
        """
        from asyncio import gather
        from aiohttp import ClientSession, TCPConnector, ClientTimeout
        from tqdm.asyncio import tqdm_asyncio

        # Create session with timeout
        timeout:   ClientTimeout = ClientTimeout(total=self.timeout)
        connector: TCPConnector  = TCPConnector(limit=self.max_concurrent)

        loop = self._format_args_into_iterables(*args)

        async with ClientSession(timeout=timeout, connector=connector) as session:
            f = lambda args: self.fetch_spectrum(session, args)
            tasks = list(map(f, zip(*loop[:3])))
            
            return await (
                tqdm_asyncio.gather(*tasks, leave=leave_pbar) \
                if self.show_progress \
                else gather(*tasks)
            )

    def __call__(
        self, 
        *args,
        leave_pbar: bool = True,
    ) -> list[tuple[ndarray, ndarray, ndarray]]:
        from itertools import batched
        from math import ceil
        from tqdm import tqdm
        from asyncio import run

        assert len(args) >= 1

        loop = self._format_args_into_iterables(*args)
        if (self.batch_size is not None) and (loop[-1] > self.batch_size):
            batch_loop = tqdm(
                zip(
                    batched(loop[0], self.batch_size),
                    batched(loop[1], self.batch_size),
                    batched(loop[2], self.batch_size),
                ),
                desc = "BATCHES",
                total = ceil(loop[-1] / self.batch_size),
            )
            out = []
            for args in batch_loop: 
                out.extend(self(*args, leave_pbar=False))

        return run(self.get_spectrum(*args, leave_pbar=leave_pbar))

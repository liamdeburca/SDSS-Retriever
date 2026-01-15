from abc import ABC, abstractmethod
from typing import Union
from pathlib import Path
from pandas import DataFrame

class SDSSParser:
    def __init__(self, path_to_catalogue: Union[str, Path]):
        _path: Path = Path(path_to_catalogue)
        assert _path.is_absolute()
        assert _path.exists()
        assert _path.name.endswith('dat')

        self.path: Path = _path
        self._cache: dict = dict((key, None) for key in self.keys())

    @staticmethod
    @abstractmethod
    def path_to_data() -> Path:
        pass

    def __getitem__(self, key: str):
        """
        Get data for the specific field. This method should be cached.
        """
        if self._cache[key] is None:
            with open(self.path, 'r') as f:
                self._cache[key] = list(
                    map(lambda line: self._parse_value(line, key), f)
                )

        return self._cache[key]

    @classmethod
    def keys(cls) -> list[str]:
        """
        Returns a list of all valid keys.
        """
        return list(cls._field_specs.keys())

    def values(self) -> list:
        """
        Returns a list of all values.
        """
        return [self[key] for key in self.keys()]

    @classmethod
    def _parse_value(
        cls,
        line: str, 
        field_name: str,
    ) -> Union[str, int, float]:
        """
        Parse a single field from a line.
        """
        
        if field_name not in cls._field_specs.keys():
            raise ValueError(f"Field name '{field_name}' is invalid!")
        
        start, end, dtype = cls._field_specs[field_name]
        value_str = line[start:end].strip()
        
        if not value_str: return None
        
        if dtype == str:     return value_str
        elif dtype == int:   return int(value_str)
        elif dtype == float: return float(value_str)

    def _clear_cache(self) -> None:
        """
        Clears the cache.
        """
        self._cache.clear()

    def _check_duplicates(self) -> dict[str, int]:
        """
        Checks for duplicate field names,
        """
        from collections import Counter
        return dict(
            item \
            for item in Counter(map(str.lower, self.keys())).items() \
            if item[1] > 1
        )
    
    def toDataFrame(
        self,
        with_duplicates: bool = False,
    ) -> DataFrame:
        """
        Creates a DataFrame with the same data.
        """
        from pandas import DataFrame

        keys = self.keys()
        if not with_duplicates:
            dupes = self._check_duplicates().keys()
            keys = filter(
                lambda key: key not in dupes,
                keys,
            )

        return DataFrame(dict(map(
            lambda key: (key.lower(), self[key]),
            keys,
        )))
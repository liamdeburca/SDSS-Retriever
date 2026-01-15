"""
Class for parsing ASCII file from Paris et al. (2018).
"""
from pathlib import Path
from .parser import SDSSParser

class Paris2018Parser(SDSSParser):
    _field_specs: dict[str, tuple] = {
        'SDSS': (0, 18, str),
        'SDSSo': (19, 37, str),
        'RAdeg': (38, 48, float),
        'DEdeg': (49, 59, float),
        'THINGID': (60, 70, int),
        'Plate': (71, 76, int),
        'MJD': (77, 82, int),
        'Fiber': (83, 87, int),
        'Note': (88, 92, str),
        'z': (93, 101, float),
        'e_z': (102, 110, float),
        'r_z': (111, 115, str),
        'z.v': (116, 125, float),
        'z.b': (126, 135, float),
        'e_z.b': (136, 147, float),
        'f_z.b': (148, 150, int),
        'z.PCA': (151, 161, float),
        'e_z.PCA': (162, 171, float),
        'zMgII': (172, 179, float),
        'Btarg1': (180, 194, int),
        'Atarg1': (195, 214, int),
        'Atarg2': (215, 232, int),
        'eBtarg0': (233, 246, int),
        'eBtarg1': (247, 257, int),
        'eBtarg2': (258, 277, int),
        'NspSDSS': (278, 279, int),
        'NspBOSS': (280, 282, int),
        'Nsp': (283, 285, int),
        'PlateDup': (285, 642, str),   # 51 x I7 array - keep as string
        'MJDDup': (642, 999, str),     # 51 x I7 array - keep as string
        'FiberDup': (999, 1305, str),  # 51 x I6 array - keep as string
        'SepDup': (1306, 1307, str),
        'BI(CIV)': (1308, 1318, float),
        'e_BI(CIV)': (1319, 1329, float),
        'RUN': (1330, 1331, int),
        'ReRUN': (1332, 1333, str),
        'Colcam': (1334, 1335, int),
        'Field': (1336, 1337, int),
        'objID': (1338, 1339, str),
        'Fu': (1340, 1352, float),
        'Fg': (1353, 1365, float),
        'Fr': (1366, 1378, float),
        'Fi': (1379, 1391, float),
        'Fz': (1392, 1404, float),
        'IVFu': (1405, 1417, float),
        'IVFg': (1418, 1430, float),
        'IVFr': (1431, 1443, float),
        'IVFi': (1444, 1456, float),
        'IVFz': (1457, 1469, float),
        'umag': (1470, 1482, float),
        'gmag': (1483, 1495, float),
        'rmag': (1496, 1508, float),
        'imag': (1509, 1521, float),
        'zmag': (1522, 1534, float),
        'e_umag': (1535, 1547, float),
        'e_gmag': (1548, 1560, float),
        'e_rmag': (1561, 1573, float),
        'e_imag': (1574, 1586, float),
        'e_zmag': (1587, 1599, float),
        'iMAG': (1600, 1611, float),
        'Extu': (1612, 1624, float),
        'Extg': (1625, 1637, float),
        'Extr': (1638, 1650, float),
        'Exti': (1651, 1663, float),
        'Extz': (1664, 1676, float),
        'logXct': (1677, 1683, float),
        'SNR(X)': (1684, 1691, float),
        'sepX': (1692, 1699, float),
        'F0.2-2': (1700, 1711, float),
        'e_F0.2-2': (1712, 1723, float),
        'F2-12': (1724, 1735, float),
        'e_F2-12': (1736, 1747, float),
        'F0.2-12': (1748, 1759, float),
        'e_F0.2-12': (1760, 1771, float),
        'L0.2-12': (1772, 1783, float),
        'sepXM': (1784, 1790, float),
        'GALEX': (1791, 1792, int),
        'FFUV': (1793, 1802, float),
        'IVFUV': (1803, 1813, float),
        'FNUV': (1814, 1823, float),
        'IVNUV': (1824, 1834, float),
        'Jmag': (1835, 1842, float),
        'e_Jmag': (1843, 1849, float),
        'SNR(J)': (1850, 1858, float),
        'f_Jmag': (1859, 1861, int),
        'Hmag': (1862, 1869, float),
        'e_Hmag': (1870, 1876, float),
        'SNR(H)': (1877, 1885, float),
        'f_Hmag': (1886, 1888, int),
        'Kmag': (1889, 1896, float),
        'e_Kmag': (1897, 1903, float),
        'SNR(K)': (1904, 1912, float),
        'f_Kmag': (1913, 1915, int),
        'Sep2': (1916, 1924, float),
        'W1mag': (1925, 1932, float),
        'e_W1mag': (1933, 1939, float),
        'W1SNR': (1940, 1947, float),
        'W1chi2': (1948, 1955, float),
        'W2mag': (1956, 1963, float),
        'e_W2mag': (1964, 1970, float),
        'W2SNR': (1971, 1979, float),
        'W2chi2': (1980, 1987, float),
        'W3mag': (1988, 1995, float),
        'e_W3mag': (1996, 2002, float),
        'W3SNR': (2003, 2011, float),
        'W3chi2': (2012, 2021, float),
        'W4mag': (2022, 2029, float),
        'e_W4mag': (2030, 2036, float),
        'W4SNR': (2037, 2044, float),
        'W4chi2': (2045, 2054, float),
        'CCflags': (2055, 2059, str),
        'PHflag': (2060, 2064, str),
        'sepW': (2065, 2073, float),
        'UKIDSS': (2074, 2075, int),
        'FY': (2076, 2088, float),
        'e_FY': (2089, 2101, float),
        'FJ': (2102, 2114, float),
        'e_FJ': (2115, 2127, float),
        'FH': (2128, 2140, float),
        'e_FH': (2141, 2153, float),
        'FK': (2154, 2166, float),
        'e_FK': (2167, 2179, float),
        'FIRST': (2180, 2182, int),
        'F1p': (2183, 2193, float),
        'SNR(1)': (2194, 2204, float),
        'sep1': (2205, 2212, float),
    }

    _zwarning_bits: dict[int, str] = {
        0: 'ZWARNING_0',
        1: 'ZWARNING_1',
        2: 'ZWARNING_2',
        3: 'ZWARNING_3',
        4: 'ZWARNING_5',
        6: 'ZWARNING_6',
        7: 'ZWARNING_7',
    }

    def __getitem__(self, key: str) -> list:
        """
        Get data for the specific field. This method is cached.
        """
        assert key in self.keys()

        if self._cache[key] is not None: return self._cache[key]
        
        # NOT a ZWARNING
        if not key.startswith('f_z.b_'):
            with open(self.path, 'r') as f:
                self._cache[key] = list(
                    map(lambda line: self._parse_value(line, key), f)
                )

            return self._cache[key]
        
        # Is a ZWARNING (it starts with 'f_z.b_')
        for zwarning_str in self._zwarning_bits.values():
            self._cache[self._format_zwarning_name(zwarning_str)] = []

        for zwarnings in map(self._parse_zwarning, self['f_z.b']):
            for item in zwarnings.items():
                self._cache[item[0]].append(item[1])

        return self._cache[key]
    
    @classmethod
    def path_to_data(cls) -> Path:
        from scripts.paris_2018.download_data import PATH_TO_DATA
        return PATH_TO_DATA
    
    @classmethod
    def keys(cls) -> list[str]:
        """
        Returns a list of all keys.
        """
        # Basic fields
        keys: list[str] = list(cls._field_specs.keys())
        # ZWARNING fields
        keys.extend(map(cls._format_zwarning_name, cls._zwarning_bits.values()))

        return keys
    
    @classmethod
    def _parse_zwarning(cls, zwarning_value: int) -> dict[str, int]:
        
        zwarnings: dict[str, int] = dict(
            (cls._format_zwarning_name(zwarning_name), 0) \
            for zwarning_name in cls._zwarning_bits.values()
        )
        for bit_position, name in cls._zwarning_bits.items():
            if bool(zwarning_value & (1 << bit_position)):
                zwarning_value[cls._format_zwarning_name(name)] = 1

        return zwarnings
    
    @classmethod
    def _format_zwarning_name(cls, zwarning_name: str) -> str:
        _zwarning_name: str = zwarning_name.removeprefix('f_z.b')
        assert _zwarning_name in cls._zwarning_bits.values()
        return f"f_z.b_{_zwarning_name}"
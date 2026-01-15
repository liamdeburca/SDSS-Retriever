"""
Class for parsing ASCII file from Shen et al. (2011).
"""
from typing import Union
from pathlib import Path
from .parser import SDSSParser

class Shen2011Parser(SDSSParser):
    _field_specs: dict[str, tuple] = {
        'SDSS': (0, 18, str),
        'RAdeg': (19, 28, float),
        'DEdeg': (29, 38, float),
        'z': (39, 47, float),
        'Plate': (48, 52, int),
        'Fiber': (53, 56, int),
        'MJD': (57, 62, int),
        'Flag': (63, 72, int),
        'N': (73, 74, int),
        'Uni': (75, 76, int),
        'iMAG': (77, 83, float),
        'logLbol': (84, 90, float),
        'e_logLbol': (91, 97, float),
        'BAL': (98, 99, int),
        'FIRST': (100, 102, int),
        'F6cm': (103, 114, float),
        'logFnu': (115, 122, float),
        'RL': (123, 133, float),
        'logL5100': (134, 140, float),
        'e_logL5100': (141, 147, float),
        'logL3000': (148, 154, float),
        'e_logL3000': (155, 161, float),
        'logL1350': (162, 168, float),
        'e_logL1350': (169, 175, float),
        'logLBHa': (176, 182, float),
        'e_logLBHa': (183, 189, float),
        'W(BHa)': (190, 197, float),
        'e_W(BHa)': (198, 205, float),
        'EWBHa': (206, 212, float),
        'e_EWBHa': (213, 221, float),
        'logLNHa': (222, 227, float),
        'e_logLNHa': (228, 233, float),
        'W(NHa)': (234, 241, float),
        'e_W(NHa)': (242, 248, float),
        'EWNHa': (249, 254, float),
        'e_EWNHa': (255, 260, float),
        'logLNII': (261, 266, float),
        'e_logLNII': (267, 272, float),
        'EWNII': (273, 278, float),
        'e_EWNII': (279, 287, float),
        'logLSII1': (288, 293, float),
        'e_logLSII1': (294, 299, float),
        'EWSII1': (300, 305, float),
        'e_EWSII1': (306, 312, float),
        'logLSII3': (313, 318, float),
        'e_logLSII3': (319, 324, float),
        'EWSII3': (325, 330, float),
        'e_EWSII3': (331, 339, float),
        'EWFeHa': (340, 345, float),
        'e_EWFeHa': (346, 351, float),
        'alpHa': (352, 358, float),
        'e_alpHa': (359, 363, float),
        'NpHa': (364, 367, int),
        'SN(Ha)': (368, 373, float),
        'chi2Ha': (374, 379, float),
        'logLBHb': (380, 385, float),
        'e_logLBHb': (386, 391, float),
        'W(BHb)': (392, 399, float),
        'e_W(BHb)': (400, 407, float),
        'EWBHb': (408, 415, float),
        'e_EWBHb': (416, 424, float),
        'logLNHb': (425, 430, float),
        'e_logLNHb': (431, 436, float),
        'W(NHb)': (437, 443, float),
        'e_W(NHb)': (444, 449, float),
        'EWNHb': (450, 455, float),
        'e_EWNHb': (456, 464, float),
        'W(NHb)g': (465, 472, float),
        'logLOIII4': (473, 477, float),
        'e_logLOIII4': (478, 482, float),
        'EWOIII4': (483, 488, float),
        'e_EWOIII4': (489, 497, float),
        'logLOIII5': (498, 502, float),
        'e_logLOIII5': (503, 507, float),
        'EWOIII5': (508, 513, float),
        'e_EWOIII5': (514, 522, float),
        'EWFeHb': (523, 530, float),
        'e_EWFeHb': (531, 539, float),
        'alpHb': (540, 546, float),
        'e_alpHb': (547, 551, float),
        'NpHb': (552, 555, int),
        'SN(Hb)': (556, 561, float),
        'chi2Hb': (562, 569, float),
        'logLMgII': (570, 575, float),
        'e_logLMgII': (576, 581, float),
        'W(MgII)': (582, 589, float),
        'e_W(MgII)': (590, 597, float),
        'EWMgII': (598, 605, float),
        'e_EWMgII': (606, 614, float),
        'logLBMgII': (615, 620, float),
        'e_logLBMgII': (621, 626, float),
        'W(BMgII)': (627, 634, float),
        'e_W(BMgII)': (635, 642, float),
        'EWBMgII': (643, 650, float),
        'e_EWBMgII': (651, 659, float),
        'W(BMgII)g': (660, 667, float),
        'FeMgII': (668, 674, float),
        'e_FeMgII': (675, 681, float),
        'alphMgII': (682, 688, float),
        'e_alphMgII': (689, 693, float),
        'NpMgII': (694, 697, int),
        'SN(MgII)': (698, 703, float),
        'chi2MgII': (704, 711, float),
        'logLCIV': (712, 717, float),
        'e_logLCIV': (718, 723, float),
        'W(CIV)': (724, 731, float),
        'e_W(CIV)': (732, 739, float),
        'EWCIV': (740, 747, float),
        'e_EWCIV': (748, 756, float),
        'alpCIV': (757, 763, float),
        'e_alpCIV': (764, 768, float),
        'NpCIV': (769, 772, int),
        'SN(CIV)': (773, 778, float),
        'chi2CIV': (779, 785, float),
        'V(BHa)': (786, 794, float),
        'e_V(BHa)': (795, 802, float),
        'V(NHa)': (803, 811, float),
        'e_V(NHa)': (812, 817, float),
        'V(BHb)': (818, 826, float),
        'e_V(BHb)': (827, 835, float),
        'V(NHb)': (836, 844, float),
        'e_V(NHb)': (845, 851, float),
        'V(BMgII)': (852, 860, float),
        'e_V(BMgII)': (861, 867, float),
        'V(CIVp)': (868, 876, float),
        'e_V(CIVp)': (877, 883, float),
        'logBHHM': (884, 889, float),
        'e_logBHHM': (890, 895, float),
        'logBHHV': (896, 901, float),
        'e_logBHHV': (902, 907, float),
        'logBHMM': (908, 913, float),
        'e_logBHMM': (914, 919, float),
        'logBHMV': (920, 925, float),
        'e_logBHMV': (926, 931, float),
        'logBHMS': (932, 937, float),
        'e_logBHMS': (938, 943, float),
        'logBHCV': (944, 949, float),
        'e_logBHCV': (950, 955, float),
        'logBH': (956, 961, float),
        'e_logBH': (962, 967, float),
        'logEdd': (968, 975, float),
        'SpF': (976, 977, int),
        'zHW': (978, 986, float),
        'e_zHW': (987, 995, float),
        'umag': (996, 1002, float),
        'gmag': (1003, 1009, float),
        'rmag': (1010, 1016, float),
        'imag': (1017, 1023, float),
        'zmag': (1024, 1030, float),
        'e_umag': (1031, 1037, float),
        'e_gmag': (1038, 1044, float),
        'e_rmag': (1045, 1051, float),
        'e_imag': (1052, 1058, float),
        'e_zmag': (1059, 1065, float),
        'umag0': (1066, 1072, float),
        'gmag0': (1073, 1079, float),
        'rmag0': (1080, 1086, float),
        'imag0': (1087, 1093, float),
        'zmag0': (1094, 1100, float),
        'Delg-i': (1101, 1109, float),
        'logNH': (1110, 1116, float),
        'CR': (1117, 1123, float),
        'oRASS': (1124, 1130, float),
        'Jmag': (1131, 1138, float),
        'Hmag': (1139, 1146, float),
        'Ksmag': (1147, 1154, float),
        'e_Jmag': (1155, 1162, float),
        'e_Hmag': (1163, 1170, float),
        'e_Ksmag': (1171, 1178, float),
        'o2M': (1179, 1184, float),
        'W1mag': (1185, 1191, float),
        'W2mag': (1192, 1198, float),
        'W3mag': (1199, 1205, float),
        'W4mag': (1206, 1212, float),
        'e_W1mag': (1213, 1219, float),
        'e_W2mag': (1220, 1226, float),
        'e_W3mag': (1227, 1233, float),
        'e_W4mag': (1234, 1240, float),
        'oWISE': (1241, 1247, float),
    }

    _flag_bits: dict[int, str] = {
        0:  'QSO_HIZ',
        1:  'QSO_CAP',
        2:  'QSO_SKIRT',
        3:  'QSO_FIRST_CAP',
        4:  'QSO_FIRST_SKIRT',
        5:  'GALAXY_RED',
        6:  'GALAXY',
        7:  'GALAXY_BIG',
        8:  'GALAXY_BRIGHT_CORE',
        9:  'ROSAT_A',
        10: 'ROSAT_B',
        11: 'ROSAT_C',
        12: 'ROSAT_D',
        13: 'STAR_BHB',
        14: 'STAR_CARBON',
        15: 'STAR_BROWN_DWARF',
        16: 'STAR_SUB_DWARF',
        17: 'STAR_CATY_VAR',
        18: 'STAR_RED_DWARF',
        19: 'STAR_WHITE_DWARF',
        20: 'SERENDIP_BLUE',
        21: 'SERENDIP_FIRST',
        22: 'SERENDIP_RED',
        23: 'SERENDIP_DISTANT',
        24: 'SERENDIP_MANUAL',
        25: 'QSO_MAG_OUTLIER',
        26: 'GALAXY_RED_II',
        27: 'ROSAT_E',
        28: 'STAR_PN',
        29: 'QSO_REJECT',
        31: 'SOUTHERN_SURVEY',
    }

    _spf_bits: dict[int, str] = {
        0: 'DISK_EMITTER',
        1: 'DISK_EMITTER_CANDIDATE',
        2: 'DOUBLE_PEAKED_OIII',
    }
    
    def __getitem__(self, key) -> list[Union[str, int, float]]:
        """
        Get data for the specific field. This method is cached.
        """
        assert key in self.keys()

        if key not in self._field_specs.keys():
            if key.startswith('Flag_'): 
                # Check if values are cached
                if self._cache[key] is None:
                    # Initialise cache values
                    for flag_str in self._flag_bits.values():
                        self._cache[self._format_flag_name(flag_str)] = []

                    for flags in map(self._parse_flag, self['Flag']):
                        for item in flags.items():
                            self._cache[item[0]].append(item[1])

            elif key.startswith('SpF_'):
                # Check if values are cached
                if self._cache[key] is None:
                    # Initialise cache values
                    for spf_str in self._spf_bits.values():
                        self._cache[self._format_spf_name(spf_str)] = []

                    for spfs in map(self._parse_spf, self['SpF']):
                        for item in spfs.items():
                            self._cache[item[0]].append(item[1])

            else:
                raise KeyError(f"Field '{key}' not found in Shen2011Parser.")
        
        elif self._cache[key] is None:
            with open(self.path, 'r') as f:
                self._cache[key] = list(
                    map(lambda line: self._parse_value(line, key), f)
                )

        return self._cache[key]
    
    @classmethod
    def path_to_data(cls) -> Path:
        from scripts.shen_2011.download_data import PATH_TO_DATA
        return PATH_TO_DATA
    
    @classmethod
    def keys(cls) -> list[str]:
        """
        Returns a list of all keys.
        """
        # Basic fields
        keys: list[str] = list(cls._field_specs.keys())
        # Flag fields
        keys.extend(map(cls._format_flag_name, cls._flag_bits.values()))
        # Special flag (SpF) fields
        keys.extend(map(cls._format_spf_name, cls._spf_bits.values()))

        return keys
    
    @classmethod
    def _parse_flag(cls, flag_value: int) -> dict[str, int]:

        flags: dict[str, int] = dict(
            (cls._format_flag_name(flag_name), 0) \
            for flag_name in Shen2011Parser._flag_bits.values()
        )
        for bit_position, name in cls._flag_bits.items():
            if bool(flag_value & (1 << bit_position)):
                flags[cls._format_flag_name(name)] = 1

        return flags
    
    @classmethod
    def _parse_spf(cls, spf_value: int) -> dict[str, int]:

        spfs: dict[str, int] = dict(
            (cls._format_spf_name(flag_name), 0) \
            for flag_name in cls._spf_bits.values()
        )
        for bit_position, name in cls._spf_bits.items():
            if bool(spf_value & (1 << bit_position)):
                spfs[cls._format_spf_name(name)] = 1

        return spfs
    
    @classmethod
    def _format_flag_name(cls, flag_name: str) -> str:
        _flag_name: str = flag_name.removeprefix('Flag_')
        assert _flag_name in cls._flag_bits.values()
        return f"Flag_{_flag_name}"
    
    @classmethod
    def _format_spf_name(cls, spf_name: str) -> str:
        _spf_name: str = spf_name.removeprefix('SpF_')
        assert _spf_name in cls._spf_bits.values()
        return f"SpF_{_spf_name}"
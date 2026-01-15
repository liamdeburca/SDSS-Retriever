"""
Submodule containing classes useful for parsing data files.
"""
from .shen_2011 import Shen2011Parser
from .paris_2017 import Paris2017Parser, Paris2017BALParser
from .paris_2018 import Paris2018Parser

PARSERS: dict = {
    'shen_2011': Shen2011Parser,
    'paris_2017': Paris2017Parser,
    'paris_2017_bal': Paris2017BALParser,
    'paris_2018': Paris2018Parser
}
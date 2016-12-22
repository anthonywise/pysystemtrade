"""
Get TradeStation equity curves from .csv files

Used for quick examples / 'scaffolding'
"""

import os

import pandas as pd
import numpy as np
import yaml
from datetime import datetime, timedelta
# import time

from syscore.fileutils import get_pathname_for_package
from syscore.fileutils import get_filename_for_package
from syscore.pdutils import pd_readcsv
from syscore.genutils import str_of_int

from sysportfolio.portfolio import Portfolio

# from collections import deque
from copy import copy  # , deepcopy

"""
Static variables to store location of data
"""
TS_PORTFOLIO_PATH = "sysportfolio.tsportfolio"


class tscsvPortfolio(Portfolio):
    """
        Get equity curve data from ts legacy csv files

        Extends the portfolio class for a specific data source

    """

    def __init__(self, tsportfoliopath=None):
        """
        Create a FuturesData object for reading .csv files from tsdatapath
        inherits from FuturesData

        We look for data in .csv files


        :param datapath: path to find .csv files (defaults to LEGACY_DATA_MODULE/TS_DATA_DIR
        :type datapath: None or str

        :returns: new tscsvFuturesData object

        >>> data=tscsvFuturesData("sysdata.tests")
        >>> data
        FuturesData object with 38 instruments


        """

        super().__init__()

        if tsportfoliopath is None:
            tsportfoliopath = TS_PORTFOLIO_PATH

        tsdatapathyaml = tsportfoliopath
        tsportfoliopath = get_pathname_for_package(tsportfoliopath)
        """
        Most Data objects that read data from a specific place have a 'source' of some kind
        Here it's a directory
        """
        setattr(self, "_tsportfoliopath", tsportfoliopath)
        setattr(self, "_tsdatapathyaml", tsdatapathyaml)
    '''
    def get_raw_price(self, instrument_code):
        """
        Get instrument price

        :param instrument_code: instrument to get prices for
        :type instrument_code: str

        :returns: pd.DataFrame

        >>> data=tscsvFuturesData("sysdata.tests")
        >>> data.get_raw_price("CORN").tail(2)
        2016-06-09    426.5
        2016-06-10    423.0
        Name: price, dtype: float64
        >>> data["US10"].tail(2)
        2015-12-11 16:06:35    126.914062
        2015-12-11 17:24:06    126.945312
        Name: price, dtype: float64
        """

        # Read from .csv
        self.log.msg("Retrieving TradeStation Daily Closing Prices for %s" % instrument_code, instrument_code=instrument_code)
        #filename = os.path.join(self._tsdatapath, instrument_code + "_data.csv")
        #instrprice = pd_readcsv(filename)
        #instrprice.columns = ["price", "open_price", "high_price", "low_price", "volume"]
        #instrprice = instrprice.groupby(level=0).last()
        instrpricedataframe = self.get_raw_daily_data(instrument_code)
        instrprice = pd.Series(instrpricedataframe.close_price)
        return instrprice
    '''
    def get_raw_equity_curve(self, system_code):
        """
        Get raw equity curve data

        :param system_code: system to get data for
        :type system_code: str

        :returns: pd.DataFrame

        >>> data=tscsvFuturesData("sysdata.tests")
        >>> data.get_raw_data("CORN").tail(2)
                    close_price  open_price  high_price  low_price  volume
        2016-06-09        426.5      430.25      430.25     423.25  262525
        2016-06-10        423.0      426.50      437.00     420.00  270454
        """

        # Read from .csv
        self.log.msg("Loading TradeStation csv equity curve for %s" % system_code, system_code=system_code)
        filename = os.path.join(self._tsportfoliopath, system_code + "_equity_curve.csv")
        equitycurve = pd.read_csv(filename, index_col=0, header=None, parse_dates=True)
        equitycurve.index = pd.to_datetime(equitycurve.index)
        equitycurve.index.name = None
        equitycurve.columns = [system_code]
        '''
        instrpricedata = pd_readcsv(filename)
        instrpricedata.columns = ["close_price", "open_price", "high_price", "low_price", "volume"]
        instrpricedata = instrpricedata.groupby(level=0).last()
        instrpricedata = pd.DataFrame(instrpricedata)
        instrpricedata = instrpricedata.asfreq('15T')  # new
        #instrpricedata.index = pd.DatetimeIndex(instrpricedata.index, freq='15T')
        '''
        return equitycurve

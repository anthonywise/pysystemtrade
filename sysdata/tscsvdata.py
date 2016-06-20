"""
Get TradeStation data from .csv files

Used for quick examples / 'scaffolding'
"""

import os

import pandas as pd

from syscore.fileutils import get_pathname_for_package
from syscore.pdutils import pd_readcsv

from sysdata.csvdata import csvFuturesData

"""
Static variables to store location of data
"""
TS_DATA_PATH = "sysdata.tscsv"


class tscsvFuturesData(csvFuturesData):
    """
        Get futures specific data from ts legacy csv files

        Extends the csvFuturesData class for a specific data source

    """

    def __init__(self, tsdatapath=None):
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

        if tsdatapath is None:
            tsdatapath = TS_DATA_PATH

        tsdatapath = get_pathname_for_package(tsdatapath)
        """
        Most Data objects that read data from a specific place have a 'source' of some kind
        Here it's a directory
        """
        setattr(self, "_tsdatapath", tsdatapath)

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
        self.log.msg("Loading TradeStation csv price for %s" % instrument_code, instrument_code=instrument_code)
        #filename = os.path.join(self._tsdatapath, instrument_code + "_data.csv")
        #instrprice = pd_readcsv(filename)
        #instrprice.columns = ["price", "open_price", "high_price", "low_price", "volume"]
        #instrprice = instrprice.groupby(level=0).last()
        instrpricedataframe = self.get_raw_data(instrument_code)
        instrprice = pd.Series(instrpricedataframe.iloc[:, 0])
        return instrprice

    def get_raw_data(self, instrument_code):
        """
        Get instrument data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: pd.DataFrame

        >>> data=tscsvFuturesData("sysdata.tests")
        >>> data.get_raw_data("CORN").tail(2)
                    close_price  open_price  high_price  low_price  volume
        2016-06-09        426.5      430.25      430.25     423.25  262525
        2016-06-10        423.0      426.50      437.00     420.00  270454
        """

        # Read from .csv
        self.log.msg("Loading TradeStation csv data for %s" % instrument_code, instrument_code=instrument_code)
        filename = os.path.join(self._tsdatapath, instrument_code + "_data.csv")
        instrpricedata = pd_readcsv(filename)
        instrpricedata.columns = ["close_price", "open_price", "high_price", "low_price", "volume"]
        instrpricedata = instrpricedata.groupby(level=0).last()
        instrpricedata = pd.DataFrame(instrpricedata)
        return instrpricedata
"""
Get TradeStation data from .csv files

Used for quick examples / 'scaffolding'
"""

import os

import pandas as pd
import numpy as np

from syscore.fileutils import get_pathname_for_package
from syscore.pdutils import pd_readcsv
from syscore.genutils import str_of_int

from sysdata.csvdata import csvFuturesData

from copy import copy, deepcopy

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

        tsdatapathyaml = tsdatapath
        tsdatapath = get_pathname_for_package(tsdatapath)
        """
        Most Data objects that read data from a specific place have a 'source' of some kind
        Here it's a directory
        """
        setattr(self, "_tsdatapath", tsdatapath)
        setattr(self., "_tsdatapathyaml", tsdatapathyaml)

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

    def get_raw_data(self, instrument_code):
        """
        Get raw instrument data

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

    def get_raw_daily_data(self, instrument_code):
        """
        Get raw instrument data

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
        self.log.msg("Loading TradeStation daily data for %s" % instrument_code, instrument_code=instrument_code)
        #filename = os.path.join(self._tsdatapath, instrument_code + "_data.csv")
        #instrpricedata = pd_readcsv(filename)
        #instrpricedata.columns = ["close_price", "open_price", "high_price", "low_price", "volume"]
        instrdailydata = copy(self.get_raw_data(instrument_code))
        #instrdailydata.groupby(level=0).last()
        dailydata = instrdailydata.resample('D').agg({'close_price': "last", 'open_price': "first", 'high_price': np.max,
                                          'low_price': np.min, 'volume': np.sum})
        #dailydata = instrdailydata.resample('D').ohlc()
        dailydata = dailydata.dropna(how='all')
        return dailydata

    def get_raw_close(self, instrument_code):
        """
        Get raw closing price

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: pd.DataFrame

        >>> data=tscsvFuturesData("sysdata.tests")
        >>> data.get_raw_close("CORN").tail(2)
        2016-06-29 19:15:00    379.5
        2016-06-29 19:20:00    379.5
        """
        instrpricedataframe = self.get_raw_data(instrument_code)
        instrprice = pd.Series(instrpricedataframe.close_price)
        return instrprice

    def _get_all_trading_hours(self):
        """
        Get a data frame of trading hours for each instrument

        :returns: pd.DataFrame

        >>> data=csvFuturesData("sysdata.tests")
        >>> data._get_all_cost_data()
                   Instrument  Slippage  PerBlock  Percentage  PerTrade
        Instrument
        BUND             BUND    0.0050      2.00           0         0
        US10             US10    0.0080      1.51           0         0
        EDOLLAR       EDOLLAR    0.0025      2.11           0         0
        """

        self.log.msg("Loading csv instrument trading hours file")

        filename = os.path.join(self._tsdatapath, "instrument_trading_hours.csv")
        try:
            instr_trading_hours = pd.read_csv(filename)
            instr_trading_hours.index = instr_trading_hours.Instrument

            return instr_trading_hours
        except OSError:
            self.log.warn("Instrument trading hours file not found %s" % filename)
            return None

    def get_instrument_raw_carry_data(self, instrument_code):
        """
        Returns a pd. dataframe with the 4 columns PRICE, CARRY, PRICE_CONTRACT, CARRY_CONTRACT

        These are specifically needed for futures trading

        :param instrument_code: instrument to get carry data for
        :type instrument_code: str

        :returns: pd.DataFrame

        >>> data=csvFuturesData("sysdata.tests")
        >>> data.get_instrument_raw_carry_data("US10").tail(4)
                                  PRICE  CARRY CARRY_CONTRACT PRICE_CONTRACT
        2015-12-10 23:00:00  126.328125    NaN         201606         201603
        2015-12-11 14:35:15  126.835938    NaN         201606         201603
        2015-12-11 16:06:35  126.914062    NaN         201606         201603
        2015-12-11 17:24:06  126.945312    NaN         201606         201603
        """

        self.log.msg("Loading csv carry data for %s" % instrument_code, instrument_code=instrument_code)

        filename = os.path.join(
            self._tsdatapath, instrument_code + "_carrydata.csv")
        instrcarrydata = pd_readcsv(filename)

        instrcarrydata = instrcarrydata.groupby(level=0).last()

        datapath = self._tsdatapathyaml + ".instr_contract_months.yaml"

        filename1 = os.path.join(
            self._tsdatapath, "contract_months.csv")

        instrcarrydata['PRICE_CONTRACT' == np.nan] = apply_contract_date(instrcarrydata, instrument_code, datapath, filename1)

        instrcarrydata['CARRY_CONTRACT'] = apply_carry_date(instrcarrydata, instrument_code)

        """
        instrcarrydata.CARRY_CONTRACT = instrcarrydata.CARRY_CONTRACT.apply(
            str_of_int)
        instrcarrydata.PRICE_CONTRACT = instrcarrydata.PRICE_CONTRACT.apply(
            str_of_int)
        """

        return instrcarrydata
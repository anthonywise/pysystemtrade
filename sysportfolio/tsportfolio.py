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

    def _get_all_cost_data(self):
        """
        Get a data frame of cost data

        :returns: pd.DataFrame

        >>> data=csvFuturesData("sysdata.tests")
        >>> data._get_all_cost_data()
                   Instrument  Slippage  PerBlock  Percentage  PerTrade
        Instrument
        BUND             BUND    0.0050      2.00           0         0
        US10             US10    0.0080      1.51           0         0
        EDOLLAR       EDOLLAR    0.0025      2.11           0         0
        """

        self.log.msg("Loading csv cost file")

        filename = os.path.join(self._tsportfoliopath, "costs_analysis.csv")
        try:
            instr_data = pd.read_csv(filename)
            instr_data.index = instr_data.Instrument

            return instr_data
        except OSError:
            self.log.warn("Cost file not found %s" % filename)
            return None

    def get_raw_cost_data(self, instrument_code):
        """
        Get's cost data for an instrument

        Get cost data

        Execution slippage [half spread] price units
        Commission (local currency) per block
        Commission - percentage of value (0.01 is 1%)
        Commission (local currency) per block

        :param instrument_code: instrument to value for
        :type instrument_code: str

        :returns: dict of floats

        >>> data=csvFuturesData("sysdata.tests")
        >>> data.get_raw_cost_data("EDOLLAR")['price_slippage']
        0.0025000000000000001
        """

        default_costs = dict(price_slippage=0.0,
                             value_of_block_commission=0.0,
                             percentage_cost=0.0,
                             value_of_pertrade_commission=0.0)

        cost_data = self._get_all_cost_data()

        if cost_data is None:
            ##
            return default_costs

        try:
            block_move_value = cost_data.loc[instrument_code, ['Slippage', 'PerBlock', 'Percentage', 'PerTrade']]
        except KeyError:
            self.log.warn("Cost data not found for %s, using zero" % instrument_code)
            return default_costs

        return dict(price_slippage=block_move_value[0],
                    value_of_block_commission=block_move_value[1],
                    percentage_cost=block_move_value[2],
                    value_of_pertrade_commission=block_move_value[3])


import numpy as np

from systems.rawdata import RawData
from syscore.objects import update_recalc
from syscore.dateutils import expiry_diff
from syscore.pdutils import  uniquets


class FuturesRawData(RawData):
    """
    A SubSystem that does futures specific raw data calculations

    KEY INPUT: system.data.get_instrument_raw_carry_data(instrument_code) found
               in self.get_instrument_raw_carry_data(self, instrument_code)

    KEY OUTPUT: system.rawdata.daily_annualised_roll(instrument_code)

    Name: rawdata
    """

    def __init__(self):
        """
        Create a futures raw data subsystem

        >>> FuturesRawData()
        SystemStage 'rawdata' futures Try objectname.methods()
        """
        super(FuturesRawData, self).__init__()

        """
        if you add another method to this you also need to add its blank dict here
        """

        protected = ['get_raw_data', 'get_raw_close', 'get_daily_data', 'get_30min_data', 'get_45min_data',
                     'get_60min_data', 'get_90min_data', 'get_120min_data', 'get_weekly_data',
                     'get_monthly_data', 'get_raw_daily_prices']
        update_recalc(self, protected)
        
        setattr(self, "description", "futures")


    def get_instrument_raw_carry_data(self, instrument_code):
        """
        Returns the 4 columns PRICE, CARRY, PRICE_CONTRACT, CARRY_CONTRACT

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.tests.testfuturesrawdata import get_test_object_futures
        >>> from systems.basesystem import System
        >>> (data, config)=get_test_object_futures()
        >>> system=System([FuturesRawData()], data)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                               PRICE  CARRY CARRY_CONTRACT PRICE_CONTRACT
        2015-12-11 17:08:14  97.9675    NaN         201812         201903
        2015-12-11 19:33:39  97.9875    NaN         201812         201903
        """

        def _calc_raw_carry(system, instrument_code, this_stage_notused):
            instrcarrydata = system.data.get_instrument_raw_carry_data(
                instrument_code)
            return instrcarrydata

        raw_carry = self.parent.calc_or_cache("instrument_raw_carry_data",
                                              instrument_code,
                                              _calc_raw_carry, self)

        return raw_carry

    def raw_futures_roll(self, instrument_code):
        """
        Returns the raw difference between price and carry

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx1 pd.DataFrame

        >>> from systems.tests.testfuturesrawdata import get_test_object_futures
        >>> from systems.basesystem import System
        >>> (data, config)=get_test_object_futures()
        >>> system=System([FuturesRawData()], data)
        >>> system.rawdata.raw_futures_roll("EDOLLAR").ffill().tail(2)
        2015-12-11 17:08:14   -0.07
        2015-12-11 19:33:39   -0.07
        dtype: float64
        """

        def _calc_raw_futures_roll(system, instrument_code, this_stage):

            carrydata = this_stage.get_instrument_raw_carry_data(
                instrument_code)
            raw_roll = carrydata.PRICE - carrydata.CARRY

            raw_roll[raw_roll == 0] = np.nan

            raw_roll=uniquets(raw_roll)
            
            return raw_roll

        raw_roll = self.parent.calc_or_cache(
            "raw_futures_roll", instrument_code, _calc_raw_futures_roll, self)

        return raw_roll

    def roll_differentials(self, instrument_code):
        """
        Work out the annualisation factor

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx1 pd.DataFrame

        >>> from systems.tests.testfuturesrawdata import get_test_object_futures
        >>> from systems.basesystem import System
        >>> (data, config)=get_test_object_futures()
        >>> system=System([FuturesRawData()], data)
        >>> system.rawdata.roll_differentials("EDOLLAR").ffill().tail(2)
        2015-12-11 17:08:14   -0.246407
        2015-12-11 19:33:39   -0.246407
        dtype: float64
        """
        def _calc_roll_differentials(system, instrument_code, this_stage):
            carrydata = this_stage.get_instrument_raw_carry_data(
                instrument_code)
            roll_diff = carrydata.apply(expiry_diff, 1)

            roll_diff = uniquets(roll_diff)

            return roll_diff

        roll_diff = self.parent.calc_or_cache(
            "roll_differentials", instrument_code, _calc_roll_differentials, self)

        return roll_diff

    def annualised_roll(self, instrument_code):
        """
        Work out annualised futures roll

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx1 pd.DataFrame

        >>> from systems.tests.testfuturesrawdata import get_test_object_futures
        >>> from systems.basesystem import System
        >>> (data, config)=get_test_object_futures()
        >>> system=System([FuturesRawData()], data)
        >>> system.rawdata.annualised_roll("EDOLLAR").ffill().tail(2)
        2015-12-11 17:08:14    0.284083
        2015-12-11 19:33:39    0.284083
        dtype: float64
        >>> system.rawdata.annualised_roll("US10").ffill().tail(2)
        2015-12-11 16:06:35    2.320441
        2015-12-11 17:24:06    2.320441
        dtype: float64

        """

        def _calc_annualised_roll(system, instrument_code, this_stage):
            rolldiffs = this_stage.roll_differentials(instrument_code)
            rawrollvalues = this_stage.raw_futures_roll(instrument_code)

            annroll = rawrollvalues /  rolldiffs

            return annroll

        annroll = self.parent.calc_or_cache(
            "annualised_roll", instrument_code, _calc_annualised_roll, self)

        return annroll

    def daily_annualised_roll(self, instrument_code):
        """
        Resample annualised roll to daily frequency

        We don't resample earlier, or we'll get bad data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx1 pd.DataFrame

        KEY OUTPUT

        >>> from systems.tests.testfuturesrawdata import get_test_object_futures
        >>> from systems.basesystem import System
        >>> (data, config)=get_test_object_futures()
        >>> system=System([FuturesRawData()], data)
        >>> system.rawdata.daily_annualised_roll("EDOLLAR").ffill().tail(2)
        2015-12-10    0.284083
        2015-12-11    0.284083
        Freq: B, dtype: float64
        """

        def _calc_daily_ann_roll(system, instrument_code, this_stage):

            annroll = this_stage.annualised_roll(instrument_code)
            annroll = annroll.resample("1B", how="mean")
            return annroll

        ann_daily_roll = self.parent.calc_or_cache(
            "daily_annualised_roll", instrument_code, _calc_daily_ann_roll, self)

        return ann_daily_roll

    def daily_denominator_price(self, instrument_code):
        """
        Gets daily prices for use with % volatility
        This won't always be the same as the normal 'price'

        :param instrument_code: Instrument to get prices for
        :type trading_rules: str

        :returns: Tx1 pd.DataFrame

        KEY OUTPUT

        >>> from systems.tests.testfuturesrawdata import get_test_object_futures
        >>> from systems.basesystem import System
        >>> (data, config)=get_test_object_futures()
        >>> system=System([FuturesRawData()], data)
        >>>
        >>> system.rawdata.daily_denominator_price("EDOLLAR").ffill().tail(2)
        2015-12-10    97.8800
        2015-12-11    97.9875
        Freq: B, Name: PRICE, dtype: float64
        """
        def _daily_denominator_prices(system, instrument_code, this_stage):
            prices = this_stage.get_instrument_raw_carry_data(
                instrument_code).PRICE
            daily_prices = prices.resample("1B", how="last") #may not need to resample
            return daily_prices

        daily_dem_prices = self.parent.calc_or_cache(
            "daily_denominator_price", instrument_code, _daily_denominator_prices, self)

        return daily_dem_prices

    def get_raw_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_raw_data(system, instrument_code, this_stage_notused):
            instrpricedata = system.data.get_raw_data(
                instrument_code)
            return instrpricedata

        raw_data = self.parent.calc_or_cache("raw_data_prices",
                                              instrument_code,
                                              _get_raw_data, self)

        return raw_data

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

        def _get_raw_close(system, instrument_code, this_stage_notused):
            instrprice = system.data.get_raw_close(
                instrument_code)
            return instrprice

        raw_close = self.parent.calc_or_cache("raw_close_prices",
                                         instrument_code,
                                         _get_raw_close, self)

        return raw_close

    def get_raw_daily_prices(self, instrument_code):
        """
        Get raw daily closing prices (not resampled to '1B')

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: pd.DataFrame

        >>> data=tscsvFuturesData("sysdata.tests")
        >>> data.get_raw_close("CORN").tail(2)
        2016-06-29 19:15:00    379.5
        2016-06-29 19:20:00    379.5
        """

        def _get_raw_daily_prices(system, instrument_code, this_stage_notused):
            rawinstrprice = system.data.get_raw_price(
                instrument_code)
            return rawinstrprice

        raw_daily_close = self.parent.calc_or_cache("raw_daily_close_prices",
                                              instrument_code,
                                              _get_raw_daily_prices, self)

        return raw_daily_close

    def get_daily_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for daily data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_daily_data(system, instrument_code, this_stage_notused):
            instrdailydata = system.data.get_raw_daily_data(
                instrument_code)
            return instrdailydata

        daily_data = self.parent.calc_or_cache("daily_data_prices",
                                             instrument_code,
                                             _get_daily_data, self)

        return daily_data

    def get_30min_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for 30min data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_30min_data(system, instrument_code, this_stage_notused):
            thirtymin_data = system.data.get_30min_data(
                instrument_code)
            return thirtymin_data

        thirty_min_data = self.parent.calc_or_cache("30min_data_prices",
                                               instrument_code,
                                               _get_30min_data, self)

        return thirty_min_data

    def get_45min_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for 45min data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_45min_data(system, instrument_code, this_stage_notused):
            fortyfivemin_data = system.data.get_45min_data(
                instrument_code)
            return fortyfivemin_data

        forty_five_min_data = self.parent.calc_or_cache("45min_data_prices",
                                                    instrument_code,
                                                    _get_45min_data, self)

        return forty_five_min_data

    def get_60min_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for 60min data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_60min_data(system, instrument_code, this_stage_notused):
            sixtymin_data = system.data.get_60min_data(
                instrument_code)
            return sixtymin_data

        sixty_min_data = self.parent.calc_or_cache("60min_data_prices",
                                                    instrument_code,
                                                    _get_60min_data, self)

        return sixty_min_data

    def get_90min_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for 90min data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_90min_data(system, instrument_code, this_stage_notused):
            ninetymin_data = system.data.get_90min_data(
                instrument_code)
            return ninetymin_data

        ninety_min_data = self.parent.calc_or_cache("90min_data_prices",
                                                    instrument_code,
                                                    _get_90min_data, self)

        return ninety_min_data

    def get_120min_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for 120min data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_120min_data(system, instrument_code, this_stage_notused):
            onehundredtwentymin_data = system.data.get_120min_data(
                instrument_code)
            return onehundredtwentymin_data

        onehundredtwenty_min_data = self.parent.calc_or_cache("120min_data_prices",
                                                    instrument_code,
                                                    _get_120min_data, self)

        return onehundredtwenty_min_data

    def get_weekly_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for weekly data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_weekly_data(system, instrument_code, this_stage_notused):
            weeklydata = system.data.get_weekly_data(
                instrument_code)
            return weeklydata

        weekly_data = self.parent.calc_or_cache("weekly_data_prices",
                                                    instrument_code,
                                                    _get_weekly_data, self)

        return weekly_data

    def get_monthly_data(self, instrument_code):
        """
        Returns the 4 columns close_price  open_price  high_price  low_price  volume
        for monthly data

        :param instrument_code: instrument to get data for
        :type instrument_code: str

        :returns: Tx4 pd.DataFrame

        KEY INPUT


        >>> from systems.provided.futures_chapter15.basesystem import futures_system
        >>> from sysdata.tscsvdata import tscsvFuturesData
        >>> mydata=tscsvFuturesData()
        >>> mysystem=futures_system(data=mydata)
        >>> mysystem.rawdata.get_raw_data("CORN").head(3)
        >>> system.rawdata.get_instrument_raw_carry_data("EDOLLAR").tail(2)
                    close_price  open_price  high_price  low_price  volume
        1996-06-12       927.00      919.50      928.50     914.50  111260
        1996-06-13       926.25      927.00      929.50     924.00   97465
        1996-06-14       914.25      925.50      925.50     914.25  113595
        """

        def _get_monthly_data(system, instrument_code, this_stage_notused):
            monthlydata = system.data.get_monthly_data(
                instrument_code)
            return monthlydata

        monthly_data = self.parent.calc_or_cache("monthly_data_prices",
                                                instrument_code,
                                                _get_monthly_data, self)

        return monthly_data

if __name__ == '__main__':
    import doctest
    doctest.testmod()

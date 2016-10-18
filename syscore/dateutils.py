"""
Various routines to do with dates
"""
import datetime
import time
import numpy as np
import pandas as pd
import yaml
from syscore.fileutils import get_filename_for_package
# from syscore.pdutils import pd_readcsv

from copy import copy

from syscore.genutils import sign

"""
First some constants
"""

CALENDAR_DAYS_IN_YEAR = 365.25

BUSINESS_DAYS_IN_YEAR = 256.0
ROOT_BDAYS_INYEAR = BUSINESS_DAYS_IN_YEAR**.5

WEEKS_IN_YEAR = CALENDAR_DAYS_IN_YEAR / 7.0
ROOT_WEEKS_IN_YEAR = WEEKS_IN_YEAR**.5

MONTHS_IN_YEAR = 12.0
ROOT_MONTHS_IN_YEAR = MONTHS_IN_YEAR**.5

AVG_HOURS_IN_TRADING_DAY = 22.0
AVG_MINUTES_IN_TRADING_DAY = AVG_HOURS_IN_TRADING_DAY * 60.0

FIFTEEN_IN_DAY = AVG_MINUTES_IN_TRADING_DAY / 15.0
ROOT_FIFTEEN_IN_DAY = FIFTEEN_IN_DAY**.5

THIRTY_IN_DAY = AVG_MINUTES_IN_TRADING_DAY / 30.0
ROOT_THIRTY_IN_DAY = THIRTY_IN_DAY**.5

FOURTYFIVE_IN_DAY = AVG_MINUTES_IN_TRADING_DAY / 45.0
ROOT_FOURTYFIVE_IN_DAY = FOURTYFIVE_IN_DAY**.5

SIXTY_IN_DAY = AVG_MINUTES_IN_TRADING_DAY
ROOT_SIXTY_IN_DAY = SIXTY_IN_DAY**.5

NINETY_IN_DAY = AVG_MINUTES_IN_TRADING_DAY / 90.0
ROOT_NINETY_IN_DAY = NINETY_IN_DAY**.5

ONETWENTY_IN_DAY = AVG_MINUTES_IN_TRADING_DAY / 120.0
ROOT_ONETWENTY_IN_DAY = ONETWENTY_IN_DAY**.5

WEEKS_IN_DAY = WEEKS_IN_YEAR / BUSINESS_DAYS_IN_YEAR
ROOT_WEEKS_IN_DAY = WEEKS_IN_DAY**.5

MONTHS_IN_DAY = MONTHS_IN_YEAR / BUSINESS_DAYS_IN_YEAR
ROOT_MONTHS_IN_DAY = MONTHS_IN_DAY**.5

ARBITRARY_START=pd.datetime(1900,1,1)

def expiry_date(expiry_ident):
    """
    Translates an expiry date which could be "20150305" or "201505" into a datetime


    :param expiry_ident: Expiry to be processed
    :type days: str or datetime.datetime

    :returns: datetime.datetime or datetime.date

    >>> expiry_date('201503')
    datetime.datetime(2015, 3, 1, 0, 0)

    >>> expiry_date('20150305')
    datetime.datetime(2015, 3, 5, 0, 0)

    >>> expiry_date(datetime.datetime(2015,5,1))
    datetime.datetime(2015, 5, 1, 0, 0)

    """

    if isinstance(expiry_ident, str):
        # do string expiry calc
        if len(expiry_ident) == 6:
            expiry_date = datetime.datetime.strptime(expiry_ident, "%Y%m")
        elif len(expiry_ident) == 8:
            expiry_date = datetime.datetime.strptime(expiry_ident, "%Y%m%d")
        else:
            raise Exception("")

    elif isinstance(expiry_ident, datetime.datetime) or isinstance(expiry_ident, datetime.date):
        expiry_date = expiry_ident

    elif isinstance(expiry_ident, pd.tslib.Timestamp):
        # expiry_date = expiry_ident.to_datetime()
        expiry_date = time.strftime("%Y%m", time.strptime(str(expiry_ident), "%Y-%m-%d %H:%M:%S"))

    else:
        raise Exception(
            "expiry_date needs to be a string with 4 or 6 digits, ")

    # 'Natural' form is datetime
    return expiry_date


def expiry_diff(carry_row, floor_date_diff=20):
    """
    Given a pandas row containing CARRY_CONTRACT and PRICE_CONTRACT, both of which represent dates

    Return the annualised difference between the dates

    :param carry_row: object with attributes CARRY_CONTRACT and PRICE_CONTRACT
    :type carry_row: pandas row, or something that quacks like it

    :param floor_date_diff: If date resolves to less than this, floor here (*default* 20)
    :type carry_row: pandas row, or something that quacks like it

    :returns: datetime.datetime or datetime.date


    """
    if carry_row.PRICE_CONTRACT == "" or carry_row.CARRY_CONTRACT == "":
        return np.nan
    ans = float((expiry_date(carry_row.CARRY_CONTRACT) -
                 expiry_date(carry_row.PRICE_CONTRACT)).days)
    if abs(ans) < floor_date_diff:
        ans = sign(ans) * floor_date_diff
    ans = ans / CALENDAR_DAYS_IN_YEAR

    return ans

class fit_dates_object(object):
    def __init__(self, fit_start, fit_end, period_start, period_end, no_data=False):
        setattr(self, "fit_start", fit_start)
        setattr(self, "fit_end", fit_end)
        setattr(self, "period_start", period_start)
        setattr(self, "period_end", period_end)
        setattr(self, "no_data", no_data)
        
    def __repr__(self):
        if self.no_data:
            return "Fit without data, use from %s to %s" % (self.period_start, self.period_end)
        else:
            return "Fit from %s to %s, use in %s to %s" % (self.fit_start, self.fit_end, self.period_start, self.period_end)
        
def generate_fitting_dates(data, date_method, rollyears=20):
    """
    generate a list 4 tuples, one element for each year in the data
    each tuple contains [fit_start, fit_end, period_start, period_end] datetime objects
    the last period will be a 'stub' if we haven't got an exact number of years
    
    date_method can be one of 'in_sample', 'expanding', 'rolling'
    
    if 'rolling' then use rollyears variable  
    """

    if date_method not in ["in_sample","rolling", "expanding"]:
        raise Exception("don't recognise date_method %s should be one of in_sample, expanding, rolling" % date_method)
    
    if type(data) is list:
        start_date=min([dataitem.index[0] for dataitem in data])
        end_date=max([dataitem.index[-1] for dataitem in data])
    else:
        start_date=data.index[0]
        end_date=data.index[-1]

    ## now generate the dates we use to fit
    if date_method=="in_sample":
        ## single period
        return [fit_dates_object(start_date, end_date, start_date, end_date)]
    
    ## generate list of dates, one year apart, including the final date
    yearstarts=list(pd.date_range(start_date, end_date, freq="12M"))+[end_date]
   
    ## loop through each period     
    periods=[]
    for tidx in range(len(yearstarts))[1:-1]:
        ## these are the dates we test in
        period_start=yearstarts[tidx]
        period_end=yearstarts[tidx+1]

        ## now generate the dates we use to fit
        if date_method=="expanding":
            fit_start=start_date
        elif date_method=="rolling":
            yearidx_to_use=max(0, tidx-rollyears)
            fit_start=yearstarts[yearidx_to_use]
        else:
            raise Exception("don't recognise date_method %s should be one of in_sample, expanding, rolling" % date_method)
            
        if date_method in ['rolling', 'expanding']:
            fit_end=period_start
        else:
            raise Exception("don't recognise date_method %s " % date_method)
        
        periods.append(fit_dates_object(fit_start, fit_end, period_start, period_end))

    if date_method in ['rolling', 'expanding']:
        #add on a dummy date for the first year, when we have no data
        periods=[fit_dates_object(start_date, start_date, start_date, yearstarts[1], no_data=True)]+periods

    return periods

'''
def apply_contract_date(carry_row, instrument_code, instrcontract_months, contract_months):
    if carry_row.index == "":
        return np.nan

    price_contract = copy(carry_row.index)

    self.log.msg("Loading csv contract months")
    contractmonthdata = pd.read_csv(contract_months).set_index('Month')

    filename = get_filename_for_package(data_path)
    with open(filename) as file_to_parse:
        dict_to_parse = yaml.load(file_to_parse)
        instr_months = dict_to_parse[instrument_code]

    price_contract = price_contract.to_datetime()
    contract_month = contractmonthdata[price_contract.month]['Code']
    contract_month = contract_month


    if isinstance(price_contract, pd.tslib.Timestamp):
        # expiry_date = expiry_ident.to_datetime()
    # expiry_date = time.strftime("%Y%m", time.strptime(str(expiry_ident), "%Y-%m-%d %H:%M:%S"))

    else:
        raise Exception(
    "expiry_date needs to be a string with 4 or 6 digits, ")

    expiry_date(carry_row.index)

    ans = float((expiry_date(carry_row.CARRY_CONTRACT) -
                 expiry_date(carry_row.PRICE_CONTRACT)).days)
    if abs(ans) < floor_date_diff:
        ans = sign(ans) * floor_date_diff
    ans = ans / CALENDAR_DAYS_IN_YEAR

    return ans
'''

if __name__ == '__main__':
    import doctest
    doctest.testmod()

import blpapi
import logging
from .BbgRefDataService import BbgRefDataService
import pandas as pd
import numpy as np
from . import BbgLogger

logger = BbgLogger.logger

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")



class BbgDataHistory(BbgRefDataService):
    def __init__(self, fields, securities, startDate, endDate, perAdjustment = "ACTUAL", perSelection = "MONTHLY", overrides = None):
        '''
            Bloomberg Historical Data query object.  Allows user to input a list of securities and fields for retrieval over a specified time period with the ability to override certain field (as specified in FLDS <GO>) if required.

        Parameters
        ----------
        fields : tuple, list, or ndarray
            The list of fields to be retrieved, field names and data types can be determined by typing FLDS <GO> and using the search box.
        securities : tuple, list, or ndarray
            List of Bloomberg tickers to retrieve data for.  If one item is passed this can be input as a string, otherwise inputs must be passed as a list or array-like.
        startDate : string
            The start date at which to retrieving data from.  Must be passed in string format as YYYYMMDD.
        endDate : string
            The end date at which to retrieving data up to.  Must be passed in string format as YYYYMMDD.
        perAdjustment : string, default ACTUAL
            Determines the frequency and calendar type of the output. To be used in conjunction with Period Selection.  Inputs include:
            1. ACTUAL : These revert to the actual date from today (if the end date is left blank) or from the end date.
            2. CALENDAR : For pricing fields, these revert to the last business day of the specified calendar period. Calendar Quarterly (CQ), Calendar Semi-Annually (CS) or Calendar Yearly (CY).
            3. FISCAL : These periods revert to the fiscal period end for the company: Fiscal Quarterly (FQ), Fiscal SemiAnnual (FS) and Fiscal Yearly (FY) only.
        perSelection : string, default MONTHLY Determines the frequency of the output. To be used in conjunction with Period Adjustment.  Inputs include DAILY, WEEKLY, MONTHLY, QUARTERLY, SEMI_ANNUAL and YEARLY.
        overrides : dictionary, optional
            A dictionary containing key, value pairs of fields and override values to input.
        
        See Also
        --------
        BbgDataHistory.constructDf : Constructor method, retrieves data associated with a BbgDataHistory query object and generates a dataframe from it.
        BbgDataPoint : Retrieve single point static, calculated or other reference data.
        BbgIntradayTick : Retrieve historic tick-level data for a given security.
        BbgIntradayBar : Retrieve historic bar level data for a given security (open, high, low and close) for a specified time interval given in minutes.

        Examples
        --------
        Constructing DataFrame of last and bid prices for ACGB 3Y and 10Y Futures.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> ACGBFutQ = bbg.BbgDataHistory(fields = ['PX_LAST', 'PX_BID'], securities = ['YM1 Comdty','XM1 Comdty'], startDate =        '20200106', endDate = '20200110', perAdjustment = 'ACTUAL', perSelection = 'DAILY')
        
        >>> ACGBFutQ.constructDf()
            Security	XM1 Comdty	YM1 Comdty	XM1 Comdty	YM1 Comdty
            Field	    PX_BID	    PX_BID	    PX_LAST	    PX_LAST
            Date				
            2020-01-06	98.775	    99.215	    98.78	    99.22
            2020-01-07	98.76	    99.215	    98.765	    99.22
            2020-01-08	98.795	    99.235	    98.8	    99.24
            2020-01-09	98.74	    99.2	    98.745	    99.205
            2020-01-10	98.725	    99.19	    98.73	    99.195
        '''
        
        self.fields = fields
        self.securities = securities
        self.startDate = startDate
        self.endDate = endDate
        self.perAdjustment = perAdjustment
        self.perSelection = perSelection
        self.overrides = overrides

    def constructDf(self):
        '''
        The constructDf method retrieves data associated with a BbgDataHistory query object and generates a dataframe from it.


        Parameters
        ----------
        None

        Returns
        -------
        table : DataFrame

        Raises
        ------
        ValueError:
            Blah blah blah
        
        See Also
        --------
        BbgDataPoint.constructDf : retrieves static point data and constructs a DataFrame from it.  It has more customisability with respect to overrides
        BbgIntradayTick.constructDf: retrieves intraday (or multi-day) tick level data and constructs a dataframe from it.  It has applications in more data intensive and granular analysis
        BbgIntradayBar.constructDf: retrieves intraday (or multi-day) bar level (open-high-low-close) data and constructs a dataframe from it.  It is for use in more data intensive and granular analysis.constructDf.  The bar interval frequency can be specified in minutes to optimise for efficiency and speed.

        Notes
        -----
        Blah blah blah
        
        Examples
        --------
        Constructing DataFrame of last and bid prices for ACGB 3Y and 10Y Futures.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> ACGBFutQ = bbg.BbgDataHistory(fields = ['PX_LAST', 'PX_BID'], securities = 
            ['YM1 Comdty','XM1 Comdty'], startDate = '20200106', endDate = '20200110', 
            perAdjustment = 'ACTUAL', perSelection = 'DAILY')
        
        >>> ACGBFutQ.constructDf()
            Security	XM1 Comdty	YM1 Comdty	XM1 Comdty	YM1 Comdty
            Field	    PX_BID	    PX_BID	    PX_LAST	    PX_LAST
            Date				
            2020-01-06	98.775	    99.215	    98.78	    99.22
            2020-01-07	98.76	    99.215	    98.765	    99.22
            2020-01-08	98.795	    99.235	    98.8	    99.24
            2020-01-09	98.74	    99.2	    98.745	    99.205
            2020-01-10	98.725	    99.19	    98.73	    99.195
        '''
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "HistoricalDataRequest")
        self.appendRequestOverrides(request = self.request, overrides = self.overrides)
        self.appendHistoricalOverrides(request = self.request, startDate = self.startDate, endDate = self.endDate, perAdjustment = self.perAdjustment, perSelection = self.perSelection)
        self.cid = self.session.sendRequest(self.request)
        self.bbgRefData = pd.DataFrame()

        for response in self.parseResponse(self.cid):
            self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response))
        
        self.bbgRefData = self.bbgRefData.set_index('Security', append = True).pivot(columns='Field').unstack('Security')
        self.bbgRefData.columns = self.bbgRefData.columns.droplevel(0).swaplevel()
        
        return self.bbgRefData

    def appendHistoricalOverrides(self, request, startDate, endDate, perAdjustment, perSelection):
        request.set("periodicityAdjustment", perAdjustment)
        request.set("periodicitySelection", perSelection)
        request.set("startDate", startDate)
        request.set("endDate", endDate)
        # request.Set("currency", "USD"); Currency: Amends the value from local to desired currency
        # request.Set("pricingOption", "PRICING_OPTION_PRICE"); Pricing Options: Sets quote to price or yield for a debt instrument whose default value is quoted in yield (depending on pricing source).
        # request.Set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS"); Non-Trading Day Fill Option: Sets to include/exclude non-trading days where no data was generated.
        # request.Set("nonTradingDayFillMethod", "PREVIOUS_VALUE"); Non-Trading Day Fill Method: If data is to be displayed for non-trading days, what data is to be returned.
        # Max Data Points: request.Set("maxDataPoints", 100); The maximum number of data points to return
        # : request.Set("calendarCodeOverride", "US"); Returns the data based on the calendar of the specified country, Exchange or religion
        return request

    def refDataContentToDf(self, response):
        securityData = response['content']['HistoricalDataResponse']['securityData']
        historicalData = securityData['fieldData']
        returnDf = pd.DataFrame()
        for snapShot in historicalData:
            fieldData = snapShot['fieldData']
            returnDf = returnDf.append(pd.DataFrame(fieldData.items(), columns=['Field', 'Values'], index = [fieldData['date'] for i in range(0, len(fieldData))])[1:])
        returnDf.index.names = ['Date']
        returnDf['Security'] = securityData['security']
        return returnDf
import datetime as dt
import blpapi
import logging
from .BbgRefDataService import BbgRefDataService
import pandas as pd
import numpy as np
from . import BbgLogger
import pytz
from tzlocal import get_localzone

logger = BbgLogger.logger

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")

BAR_DATA = blpapi.Name("barData")
BAR_TICK_DATA = blpapi.Name("barTickData")
OPEN = blpapi.Name("open")
HIGH = blpapi.Name("high")
LOW = blpapi.Name("low")
CLOSE = blpapi.Name("close")
VOLUME = blpapi.Name("volume")
NUM_EVENTS = blpapi.Name("numEvents")
TIME = blpapi.Name("time")

class BbgIntradayBar(BbgRefDataService):
    def __init__(self, securities, startTime, endTime, event = "TRADE", barInterval = 60, timeZone = str(get_localzone()), gapFillInitialBar = False, adjustmentSplit = True, adjustmentAbnormal = False, adjustmentNormal = False, adjustmentFollowDPDF = True):
        '''
            Bloomberg Intraday Bar query object.  Allows user to input a list of securities retrieval over a specified time period subject to the usual constraints that apply to Bloomberg Intraday Bar data retrieval.

        Parameters
        ----------
        fields : tuple, list, or ndarray
            The list of fields to be retrieved, field names and data types can be determined by typing FLDS <GO> and using the search box.
        securities : tuple, list, or ndarray
            List of Bloomberg tickers to retrieve data for.  If one item is passed this can be input as a string, otherwise inputs must be passed as a list or array-like.
        startTime : datetime.datetime
            The start date and time at which to retrieving data from.  Must be passed as a datetime.
        endTime : datetime.datetime
            The end date and time at which to retrieving data from.  Must be passed as a datetime.
        event : string
            Defines the market event supplied for an intraday request.  Could be TRADE, BID or ASK.  If no event is passed, will default to TRADE.
        barInterval : integer
            Sets the length of each time-bar in the response. Entered as a whole number (between 1 and 1,440 minutes). If omitted, the request will default to 60 minutes. One minute is the lowest possible granularity.
        timeZone : string
            Timezone for the request based on the pytz package timezone names.  If no timezone is passed, will default to current system timezone.
        gapFillInitialBar : bool
            Adjust historical pricing to reflect: Special Cash, Liquidation, Capital Gains, Long-Term Capital Gains, Short-Term Capital Gains, Memorial, Return of Capital, Rights Redemption, Miscellaneous, Return Premium, Preferred Rights Redemption, Proceeds/Rights, Proceeds/Shares, Proceeds/Warrants
        adjustmentSplit : bool
            Adjust historical pricing and/or volume to reflect: Spin-Offs, Stock Splits/Consolidations, Stock Dividend/Bonus, Rights Offerings/Entitlement.  If not set, will be set to True.
        adjustmentAbnormal : bool
            Adjust historical pricing to reflect: Special Cash, Liquidation, Capital Gains, Long-Term Capital Gains, Short-Term Capital Gains, Memorial, Return of Capital, Rights Redemption, Miscellaneous, Return Premium, Preferred Rights Redemption, Proceeds/Rights, Proceeds/Shares, Proceeds/Warrants.  If not set, will be set to False.
        adjustmentNormal : bool
            Adjust historical pricing to reflect: Regular Cash, Interim, 1st Interim, 2nd Interim, 3rd Interim, 4th Interim, 5th Interim, Income, Estimated, Partnership Distribution, Final, Interest on Capital, Distribution, Prorated.  If not set, will be set to False.
        adjustmentFollowDPDF : bool
            Setting to True will follow the DPDF <GO> Terminal function. True is the default setting for this option.  If not set, will be set to True.
        
        See Also
        --------
        BbgIntradayBar.constructDf : Constructor method, retrieves data associated with a BbgDataPoint query object and generates a dataframe from it.
        BbgDataPoint : Retrieve single point static, calculated or other reference data.
        BbgIntradayTick : Retrieve historic tick-level data for a given security.
        BbgIntradayBar : Retrieve historic bar level data for a given security (open, high, low and close) for a specified time interval given in minutes.

        Examples
        --------
        Retrieve open, high, low, close, volume, number of events and value data for a basket of securities between two datetimes.

        >>> import datetime as dt
        
        >>> import pandas as pd
        
        >>> import BloombergDataModule as bbg

        >>> futHist = bbg.BbgIntradayBar(securities = ["YMH0 Comdty", "XMH0 Comdty"], startTime = dt.datetime(2020, 1, 31, 9, 0, 0), endTime = dt.datetime(2020, 1, 31, 12, 0, 0), barInterval = 5)
        
        >>> futHist.constructDf().head()
            Field	                                    open	high	low	    close	volume	numEvents	value
            Security	    time							
            YMH0 Comdty	    2020-01-31 09:10:00+11:00	99.37	99.375	99.37	99.375	149	    3	        14806.3
                            2020-01-31 09:15:00+11:00	99.375	99.38	99.375	99.38	1749	13	        173807
                            2020-01-31 09:20:00+11:00	99.38	99.38	99.38	99.38	6	    6	        596.28
                            2020-01-31 09:25:00+11:00	99.38	99.38	99.375	99.38	2170	35	        215655
                            2020-01-31 09:30:00+11:00	99.38	99.38	99.375	99.38	93	    3	        9241.89
        '''
        self.securities = list(securities) if type(securities) is not list else securities
        self.startTime = startTime
        self.endTime = endTime
        self.event = event
        self.barInterval = barInterval
        self.timeZone = timeZone
        self.gapFillInitialBar = gapFillInitialBar
        self.adjustmentSplit = adjustmentSplit
        self.adjustmentAbnormal = adjustmentAbnormal
        self.adjustmentNormal = adjustmentNormal
        self.adjustmentFollowDPDF = adjustmentFollowDPDF

    def constructDf(self):
        '''
        The constructDf method retrieves data associated with a BbgIntradayBar query object and generates a dataframe from it.

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
        BbgDataHistory.constructDf : retrieves static history data and constructs a DataFrame from it.  It has more customisability with respect to overrides
        BbgIntradayTick.constructDf: retrieves intraday (or multi-day) tick level data and constructs a dataframe from it.  It has applications in more data intensive and granular analysis
        BbgDataPoint.constructDf: retrieves intraday (or multi-day) bar level (open-high-low-close) data and constructs a dataframe from it.  It is for use in more data intensive and granular analysis.constructDf.  The bar interval frequency can be specified in minutes to optimise for efficiency and speed.

        Notes
        -----
        Blah blah blah
        
        Examples
        --------
        Retrieve open, high, low, close, volume, number of events and value data for a basket of securities between two datetimes.

        >>> import datetime as dt
        
        >>> import pandas as pd
        
        >>> import BloombergDataModule as bbg

        >>> futHist = bbg.BbgIntradayBar(securities = ["YMH0 Comdty", "XMH0 Comdty"], startTime = dt.datetime(2020, 1, 31, 9, 0, 0), endTime = dt.datetime(2020, 1, 31, 12, 0, 0), barInterval = 5)
        
        >>> futHist.constructDf().head()
            Field	                                    open	high	low	    close	volume	numEvents	value
            Security	    time							
            YMH0 Comdty	    2020-01-31 09:10:00+11:00	99.37	99.375	99.37	99.375	149	    3	        14806.3
                            2020-01-31 09:15:00+11:00	99.375	99.38	99.375	99.38	1749	13	        173807
                            2020-01-31 09:20:00+11:00	99.38	99.38	99.38	99.38	6	    6	        596.28
                            2020-01-31 09:25:00+11:00	99.38	99.38	99.375	99.38	2170	35	        215655
                            2020-01-31 09:30:00+11:00	99.38	99.38	99.375	99.38	93	    3	        9241.89
        '''
        BbgRefDataService.__init__(self)
        self.bbgRefData = pd.DataFrame()

        UTCStartTime = self.__convertFromTimezoneToUTC(self.startTime, self.timeZone)
        UTCEndTime = self.__convertFromTimezoneToUTC(self.endTime, self.timeZone)


        for sec in self.securities:
            self.request = self.createIntradayBarRequest(security = sec, requestType = "IntradayBarRequest", startTime = UTCStartTime, endTime = UTCEndTime, event = self.event, barInterval = self.barInterval, gapFillInitialBar = self.gapFillInitialBar, adjustmentSplit = self.adjustmentSplit, adjustmentAbnormal = self.adjustmentAbnormal, adjustmentNormal = self.adjustmentNormal, adjustmentFollowDPDF = self.adjustmentFollowDPDF)
            self.cid = self.session.sendRequest(self.request)
            for response in self.parseResponse(self.cid, False):
                self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response, sec))
        BbgRefDataService.__del__(self)
        self.bbgRefData['time'] = self.bbgRefData['time'].apply(lambda x: self.__convertFromUTCToTimezone(x, self.timeZone))
        return self.bbgRefData.set_index(['Security', 'time'])

    def refDataContentToDf(self, response, security):
        securityData = response['content']['IntradayBarResponse']['barData']
        barData = securityData['barTickData']
        returnDf = pd.DataFrame()
        for snapShot in barData:
            fieldData = snapShot['barTickData']
            returnDf = returnDf.append(pd.DataFrame(fieldData.items(), columns=['Field', 'Values']).set_index('Field').transpose().reset_index(drop=True))#, index = [fieldData['time'] for i in range(0, len(fieldData))])[1:])
        returnDf.index.names = ['time']
        returnDf['Security'] = security
        return returnDf

    def __convertFromUTCToTimezone(self, fromDt, toTimeZone):
        return pytz.utc.localize(fromDt).astimezone(pytz.timezone(toTimeZone))

    def __convertFromTimezoneToUTC(self, fromDt, fromTimeZone):
        return pytz.timezone(fromTimeZone).localize(fromDt).astimezone(pytz.utc)
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

class BbgDataService(BbgRefDataService):
    def __init__(self, field, securities, overrides = None):
        '''
        Bloomberg Bulk Reference Data query object.  Allows user to input a list of securities and fields for retrieval over a specified time period with the ability to override certain field (as specified in FLDS <GO>) if required.

        Parameters
        ----------
        field : string
            The field to be retrieved, field names and data types can be determined by typing FLDS <GO> and using the search box.  If more than one field is provided will raise a TypeError
        securities : string, tuple, list, or ndarray
            List of Bloomberg tickers to retrieve data for.  If one item is passed this can be input as a string, otherwise inputs must be passed as a list or array-like.
        overrides : dictionary, optional
            A dictionary containing key, value pairs of fields and override values to input.
        
        See Also
        --------
        BbgDataService.constructDf : Constructor method, retrieves data associated with a BbgDataService query object and generates a dataframe from it.
        BbgDataPoint : Retrieve single point static, calculated or other reference data.
        BbgIntradayTick : Retrieve historic tick-level data for a given security.
        BbgIntradayBar : Retrieve historic bar level data for a given security (open, high, low and close) for a specified time interval given in minutes.

        Examples
        --------
        Constructing DataFrame of last and bid prices for ACGB 3Y and 10Y Futures.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> curveTenorRates = bbg.BbgDataService(fields = ['CURVE_TENOR_RATES'], securities = ['YCGT0025 Index','YCGT0016 Index',      'YCGT0001 Index'], overrides = {'CURVE_DATE': '20060830'})
        
        >>> curveTenorRates.constructDf().head()
            	            Ask Yield	Bid Yield	Last Update	    Mid Yield	Tenor	Tenor Ticker
        BB_TICKER						
        YCGT0025 Index	    5.041	    5.051	    2006-08-30	    5.046	    3M	    912795YG Govt
        YCGT0025 Index	    5.126	    5.137	    2006-08-30	    5.132	    6M	    912795YV Govt
        YCGT0025 Index	    4.809	    4.817	    2006-08-30	    4.813	    2Y	    912828FR Govt
        YCGT0025 Index	    4.737	    4.742	    2006-08-30	    4.740	    3Y	    912828FP Govt
        YCGT0025 Index	    4.723	    4.727	    2006-08-30	    4.725	    5Y	    912828FN Govt
        '''
        self.fields = list(field) if type(field) is not list else field
        if len(self.fields) > 1:
            raise TypeError("BbgDataService is only designed to handle a single bulk field per request.")
        self.securities = securities
        self.overrides = overrides

    def constructDf(self):
        '''
        The constructDf method retrieves data associated with a BbgDataService query object and generates a dataframe from it.

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
        Constructing DataFrame of last and bid prices for ACGB 3Y and 10Y Futures.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> curveTenorRates = bbg.BbgDataService(fields = ['CURVE_TENOR_RATES'], securities = ['YCGT0025 Index','YCGT0016 Index',      'YCGT0001 Index'], overrides = {'CURVE_DATE': '20060830'})
        
        >>> curveTenorRates.constructDf().head()
            	            Ask Yield	Bid Yield	Last Update	    Mid Yield	Tenor	Tenor Ticker
        BB_TICKER						
        YCGT0025 Index	    5.041	    5.051	    2006-08-30	    5.046	    3M	    912795YG Govt
        YCGT0025 Index	    5.126	    5.137	    2006-08-30	    5.132	    6M	    912795YV Govt
        YCGT0025 Index	    4.809	    4.817	    2006-08-30	    4.813	    2Y	    912828FR Govt
        YCGT0025 Index	    4.737	    4.742	    2006-08-30	    4.740	    3Y	    912828FP Govt
        YCGT0025 Index	    4.723	    4.727	    2006-08-30	    4.725	    5Y	    912828FN Govt
        '''
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "ReferenceDataRequest")
        self.request = self.appendRequestOverrides(request = self.request, overrides = self.overrides)
        self.cid = self.session.sendRequest(request = self.request)
        self.bbgRefData = pd.DataFrame()

        for response in self.parseResponse(self.cid):
            self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response))
        
        return self.bbgRefData

    def refDataContentToDf(self, response):
        responseData = response['content']['ReferenceDataResponse']
        returnDf = pd.DataFrame()
        tempDf = pd.DataFrame()
        for security in responseData:
            securityData = security['securityData']
            fieldData = securityData['fieldData']['fieldData']
            tempDf = pd.DataFrame()
            for fieldK, fieldV in fieldData.items():
                for val in fieldV:
                    tempDf = tempDf.append(pd.DataFrame(val.values()),sort=True)
                    
            tempDf['BB_TICKER'] = securityData['security']
            
            returnDf = returnDf.append(tempDf,sort=True)
            
        return returnDf.set_index("BB_TICKER")
    
    def inspectResponse(self):
        responseList = []
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "ReferenceDataRequest")
        self.request = self.appendRequestOverrides(self.request, self.overrides)
        self.cid = self.session.sendRequest(self.request)
        for response in self.parseResponse(self.cid):
            responseList.append(response)
        return responseList
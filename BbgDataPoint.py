import blpapi
import logging
from .BbgRefDataService import BbgRefDataService
import pandas as pd
import numpy as np
import BbgLogger

SECURITY_DATA = blpapi.Name("securityData")
SECURITY = blpapi.Name("security")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
ERROR_INFO = blpapi.Name("errorInfo")

logger = BbgLogger.logger

class BbgDataPoint(BbgRefDataService):
    def __init__(self, fields, securities, overrides = None):
        '''
            Bloomberg Historical Data query object.  Allows user to input a list of securities and fields for retrieval over a specified time period with the ability to override certain field (as specified in FLDS <GO>) if required.

        Parameters
        ----------
        fields : tuple, list, or ndarray
            The list of fields to be retrieved, field names and data types can be determined by typing FLDS <GO> and using the search box.
        securities : tuple, list, or ndarray
            List of Bloomberg tickers to retrieve data for.  If one item is passed this can be input as a string, otherwise inputs must be passed as a list or array-like.
        overrides : dictionary, optional
            A dictionary containing key, value pairs of fields and override values to input.
        
        See Also
        --------
        BbgDataPoint.constructDf : Constructor method, retrieves data associated with a BbgDataPoint query object and generates a dataframe from it.
        BbgDataPoint : Retrieve single point static, calculated or other reference data.
        BbgIntradayTick : Retrieve historic tick-level data for a given security.
        BbgIntradayBar : Retrieve historic bar level data for a given security (open, high, low and close) for a specified time interval given in minutes.

        Examples
        --------
        Retrieve last, high and low price data for a basket of stocks in the current session.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> stockPriceData = bbg.BbgDataPoint(securities = ['MSFT US Equity', 'IBM US Equity', 'AMP AU Equity'],fields =               ['PX_LAST', 'PX_HIGH', 'PX_LOW'])
        
        >>> stockPriceData.constructDf()
            Fields	            PX_HIGH	    PX_LAST	    PX_LOW
            securities			
            AMP AU Equity	    1.84	    1.825	    1.815
            IBM US Equity	    144.05	    143.730	    140.790
            MSFT US Equity	    172.40	    170.230	    169.580

        Retrieve bid and ask forward yield to convention for the 3-Year bond futures basket @ the futures settlement date

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> basketBondA = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 104.0189, "PX_ASK": 104.0339})
        >>> basketBondB = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 113.8049, "PX_ASK": 113.8199})
        >>> basketBondC = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 107.8918, "PX_ASK": 107.9968})

        >>> outData = pd.DataFrame()
        >>> outData = outData.append(basketBondA.constructDf())
        >>> outData = outData.append(basketBondB.constructDf())
        >>> outData = outData.append(basketBondC.constructDf())

        >>> outData
            Fields	            YLD CNV ASK     YLD CNV BID
            securities		
            AP364296 Corp   	0.577168	    0.583240
            AP364296 Corp   	-3.170604	    -3.165165
            AP364296 Corp   	-0.990407	    -0.949785
        '''
        self.fields = fields
        self.securities = securities
        self.overrides = overrides
        
    def constructDf(self):
        '''
        The constructDf method retrieves data associated with a BbgDataPoint query object and generates a dataframe from it.


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
        BbgIntradayBar.constructDf: retrieves intraday (or multi-day) bar level (open-high-low-close) data and constructs a dataframe from it.  It is for use in more data intensive and granular analysis.constructDf.  The bar interval frequency can be specified in minutes to optimise for efficiency and speed.

        Notes
        -----
        Blah blah blah
        
        Examples
        --------
        Retrieve last, high and low price data for a basket of stocks in the current session.

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> stockPriceData = bbg.BbgDataPoint(securities = ['MSFT US Equity', 'IBM US Equity', 'AMP AU Equity'],fields =               ['PX_LAST', 'PX_HIGH', 'PX_LOW'])
        
        >>> stockPriceData.constructDf()
            Fields	            PX_HIGH	    PX_LAST	    PX_LOW
            securities			
            AMP AU Equity	    1.84	    1.825	    1.815
            IBM US Equity	    144.05	    143.730	    140.790
            MSFT US Equity	    172.40	    170.230	    169.580

        Retrieve bid and ask forward yield to convention for the 3-Year bond futures basket @ the futures settlement date

        >>> import BloombergDataModule as bbg

        >>> import pandas as pd

        >>> basketBondA = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 104.0189, "PX_ASK": 104.0339})
        >>> basketBondB = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 113.8049, "PX_ASK": 113.8199})
        >>> basketBondC = bbg.BbgDataPoint(securities = ['AP364296 Corp'],fields = ['YLD CNV BID', 'YLD CNV ASK'], overrides =         {'SETTLE_DT': "20200615", "PX_BID": 107.8918, "PX_ASK": 107.9968})

        >>> outData = pd.DataFrame()
        >>> outData = outData.append(basketBondA.constructDf())
        >>> outData = outData.append(basketBondB.constructDf())
        >>> outData = outData.append(basketBondC.constructDf())

        >>> outData
            Fields	            YLD CNV ASK     YLD CNV BID
            securities		
            AP364296 Corp   	0.577168	    0.583240
            AP364296 Corp   	-3.170604	    -3.165165
            AP364296 Corp   	-0.990407	    -0.949785
        '''
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "ReferenceDataRequest")
        self.request = self.appendRequestOverrides(self.request, self.overrides)
        self.cid = self.session.sendRequest(self.request)
        self.bbgRefData = pd.DataFrame()
        for response in self.parseResponse(self.cid):
            self.bbgRefData = self.bbgRefData.append(self.refDataContentToDf(response))
        return self.bbgRefData

    def refDataContentToDf(self, response):
        responseContent = response['content']
        referenceData = responseContent['ReferenceDataResponse']
        returnDf = pd.DataFrame()
        for item in referenceData:
            tempDf = pd.DataFrame(item['securityData']['fieldData']['fieldData'].items(), columns=['Fields', 'Values'])
            tempDf['securities'] = item['securityData']['security']
            returnDf = returnDf.append(tempDf)
        return returnDf.pivot(index = 'securities', columns = 'Fields', values = 'Values')
    
    def inspectReponse(self):
        responseList = []
        BbgRefDataService.__init__(self)
        self.request = self.createRequest(securities = self.securities, fields = self.fields, requestType = "ReferenceDataRequest")
        self.request = self.appendRequestOverrides(self.request, self.overrides)
        self.cid = self.session.sendRequest(self.request)
        for response in self.parseResponse(self.cid):
            responseList.append(response)
        return responseList

    

    
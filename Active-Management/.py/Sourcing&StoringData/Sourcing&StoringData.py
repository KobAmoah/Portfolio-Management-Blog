# File: Sourcing&StoringData.py
# Description: This file sources data from the Fin Model Prep API and stores the data in a remote MySQL Database
# Fin Model Prep:   https://github.com/antoinevulcain/Financial-Modeling-Prep-API
#
# Author: Kobby Amoah <amoahkobena@gmail.com>
# Copyright (c) 2023 
#
# Licensed under the MIT License.
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   https://opensource.org/license/mit/
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
#
######################################################################
######################################################################

#!/usr/bin/env python3

# Library Imports
import sys
if not sys.warnoptions:   # Note this hides all Python level warnings from their users by default
    import warnings
    warnings.simplefilter("ignore")


from urllib.request import urlopen
from urllib.error import  (HTTPError, URLError)
from pathlib import Path
import requests
import json

# Progress bar
from tqdm import tqdm

# Data Manipulation
import pandas as pd
import pyarrow.parquet as pq

# Database
import pymysql
from datetime import datetime
from sqlalchemy import (MetaData,Table,Column, Integer, Numeric,String, DateTime,
                        ForeignKey,create_engine,insert)

from typing import Optional

# store data in this directory
data_path = Path('data') 
if not data_path.exists():
    data_path.mkdir()


def get_jsonparsed_data(url: str, field: Optional[str] = None):
   '''Returns API data in a usable format(As a DataFrame)'''
   try:
       response = urlopen(url).read().decode("utf-8")
   except (URLError, HTTPError):
       return None
   data = json.loads(response)
   
   if field == None:
       return pd.DataFrame(data)
   return pd.DataFrame(data.get(field))


###########################################################################
                       # Data Download
###########################################################################

##### Tickers List Download
api_key = # Enter your api key
url = f"https://financialmodelingprep.com/api/v3/available-traded/list?apikey={api_key}"

ticker_list = get_jsonparsed_data(url)
ticker_list = ticker_list.query("exchangeShortName == 'NASDAQ'  or exchangeShortName == 'NYSE' or exchangeShortName == 'AMEX' ")
ticker_list = ticker_list.query("type=='stock'")
symbols = ticker_list[['symbol','name']]
symbol_list = symbols.symbol

##### Fin Ratios Data Download
df_list = []
quarters = 40
print('\nFinancial Ratio Data Download\n')
for ticker in tqdm(symbol_list):    
    # define url for each ticker and get financial ratio data
    try:
        url =  f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?period=quarter&limit={quarters}&apikey={api_key}" 
        dict_data = get_jsonparsed_data(url)
        df_list.extend(dict_data)
        
    except URLError: continue
        
fin_ratio_df = pd.concat(df_list, ignore_index=True)

# Set (and create) directory
path = data_path / f'stock_fin_ratios'
if not path.exists():
    path.mkdir(parents=True)
    
# save to parquet.gzip file
fin_ratio_df.to_parquet(f'stock_fin_ratios.parquet.gzip')

##### Price Data Download - Resampled to Monthly
df_list = []
print('\nPrice Data Download\n')
for ticker in tqdm(symbol_list):
    # define url for each ticker and get price data
    try:
        url =  f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={api_key}" 
        dict_data = get_jsonparsed_data(url,'historical')
        df = get_jsonparsed_data(url).iloc[:, 0]
        df = pd.concat([df,dict_data],axis='columns')   
        df = (df
            .set_index('date') 
            .resample('M')
            .last()
            .reset_index())
        df_list.extend(df)
        
    except URLError: continue     

price_df = pd.concat(df_list, ignore_index=True)

# Set (and create) directory
path = data_path / f'stock_prices'
if not path.exists():
    path.mkdir(parents=True)
    
# save to parquet.gzip file
price_df.to_parquet(f'price_df.parquet.gzip')


##### Fin Growth Data Download
df_list = []
quarters = 40
print('\nFinancial Growth Data Download\n')
for ticker in tqdm(symbol_list):    
    # define url for each ticker and get financial growth data
    try:
        url =  f"https://financialmodelingprep.com/api/v3/financial-growth/{ticker}?period=quarter&limit={quarters}&apikey={api_key}"
        dict_data = get_jsonparsed_data(url)
        df_list.extend(dict_data)
        
    except URLError: continue
        
fin_growth_df = pd.concat(df_list, ignore_index=True)

# Set (and create) directory
path = data_path / f'stock_fin_growth'
if not path.exists():
    path.mkdir(parents=True)
    
# save to parquet.gzip file
fin_growth_df.to_parquet(f'stock_fin_growth.parquet.gzip')


###########################################################################
                       # Data Storage
###########################################################################

##### Database storage - SQL - Notice I use a remote MySQL database

user = # Enter your username
password = # Enter your password
hostname= # Enter your hostname
projectname = # Enter your project name
address = f'mysql+pymysql://{user}:{password}@{hostname}/{projectname}'

metadata = MetaData()

stock_names = Table('stock_names',metadata,
          Column('stock_symbol',primary_key= True,unique=True),
          Column('stock_name',String(255)))

price_data = Table('price_data',metadata,
         Column('stock_symbol',ForeignKey('stock_names.stock_symbol'),primary_key=True),
         Column('date',DateTime(),index = True),
         Column('open',Numeric(12,2)),
         Column('high',Numeric(12,2)),
         Column('low',Numeric(12,2)),
         Column('close',Numeric(12,2)),
         Column('adjClose',Numeric(12,2)),
         Column('volume',Integer()),
         Column('unadjustedVolume',Integer()),
         Column('changePercent',Numeric(12,2)),
         Column('vwap',Numeric(12,2)),
         Column('label',DateTime()),
         Column('changeOverTime',Numeric(12,2)),
         Column('created_on',DateTime(),default = datetime.now),  
         Column('updated_on',DateTime(),default = datetime.now, onupdate=datetime.now) )

fin_ratio = Table('fin_ratio',metadata,
         Column('stock_symbol',ForeignKey('stock_names.stock_symbol'),primary_key=True),
         Column('date', DateTime(),index = True),
         Column('period', String(50)),
         Column('currentRatio', Numeric(12,2)),
         Column('quickRatio', Numeric(12,2)),
         Column('cashRatio', Numeric(12,2)),
         Column('daysOfSalesOutstanding', Numeric(12,2)),
         Column('daysOfInventoryOutstanding', Numeric(12,2)),
         Column('operatingCycle', Numeric(12,2)),
         Column('daysOfPayablesOutstanding', Numeric(12,2)),
         Column('cashConversionCycle', Numeric(12,2)),
         Column('grossProfitMargin', Numeric(12,2)),
         Column('operatingProfitMargin', Numeric(12,2)),
         Column('pretaxProfitMargin', Numeric(12,2)),
         Column('netProfitMargin', Numeric(12,2)),
         Column('effectiveTaxRate', Numeric(12,2)),
         Column('returnOnAssets', Numeric(12,2)),
         Column('returnOnEquity', Numeric(12,2)),
         Column('returnOnCapitalEmployed', Numeric(12,2)),
         Column('netIncomePerEBT', Numeric(12,2)),
         Column('ebtPerEbit', Numeric(12,2)),
         Column('ebitPerRevenue', Numeric(12,2)),
         Column('debtRatio', Numeric(12,2)),
         Column('debtEquityRatio', Numeric(12,2)),
         Column('longTermDebtToCapitalization', Numeric(12,2)),
         Column('totalDebtToCapitalization', Numeric(12,2)),
         Column('interestCoverage',Numeric(12,2)),
         Column('cashFlowToDebtRatio',Numeric(12,2)),
         Column('companyEquityMultiplier',Numeric(12,2)),
         Column('receivablesTurnover',Numeric(12,2)),
         Column('payablesTurnover',Numeric(12,2)),
         Column('inventoryTurnover',Numeric(12,2)),
         Column('fixedAssetTurnover',Numeric(12,2)),
         Column('payablesTurnover',Numeric(12,2)),
         Column('inventoryTurnover',Numeric(12,2)),
         Column('fixedAssetTurnover',Numeric(12,2)),
         Column('assetTurnover',Numeric(12,2)),
         Column('operatingCashFlowPerShare',Numeric(12,2)),
         Column('freeCashFlowPerShare',Numeric(12,2)),
         Column('cashPerShare', Numeric(12,2)),
         Column('payoutRatio', Numeric(12,2)),
         Column('operatingCashFlowSalesRatio', Numeric(12,2)),
         Column('freeCashFlowOperatingCashFlowRatio', Numeric(12,2)),
         Column('cashFlowCoverageRatios', Numeric(12,2)),
         Column('shortTermCoverageRatios', Numeric(12,2)),
         Column('capitalExpenditureCoverageRatio', Numeric(12,2)),
         Column('dividendPaidAndCapexCoverageRatio', Numeric(12,2)),
         Column('dividendPayoutRatio', Numeric(12,2)),
         Column('priceBookValueRatio', Numeric(12,2)),
         Column('priceToBookRatio', Numeric(12,2)),
         Column('priceToSalesRatio', Numeric(12,2)),
         Column('priceEarningsRatio', Numeric(12,2)),
         Column('priceToFreeCashFlowsRatio', Numeric(12,2)),
         Column('priceToOperatingCashFlowsRatio', Numeric(12,2)),
         Column('priceCashFlowRatio', Numeric(12,2)),
         Column('priceEarningsToGrowthRatio', Numeric(12,2)),
         Column('priceSalesRatio', Numeric(12,2)),
         Column('dividendYield', Numeric(12,2)),
         Column('enterpriseValueMultiple', Numeric(12,2)),
         Column('priceFairValue', Numeric(12,2)),
         Column('created_on',DateTime(),default = datetime.now),  
         Column('updated_on',DateTime(),default = datetime.now, onupdate=datetime.now) ) 

fin_growth = Table('fin_growth',metadata,
         Column('stock_symbol',ForeignKey('stock_names.stock_symbol'),primary_key=True),
         Column('date',DateTime(),index = True),
         Column('period', String(50)),
         Column('revenueGrowth', Numeric(12,2)),
         Column('grossProfitGrowth',Numeric(12,2)),
         Column('ebitgrowth', Numeric(12,2)),
         Column('operatingIncomeGrowth',Numeric(12,2)),
         Column('netIncomeGrowth',Numeric(12,2)),
         Column('epsgrowth', Numeric(12,2)),
         Column('epsdilutedGrowth',Numeric(12,2)),
         Column('weightedAverageSharesGrowth',Numeric(12,2)),
         Column('weightedAverageSharesDilutedGrowth',Numeric(12,2)),
         Column('dividendperShareGrowth', Numeric(12,2)),
         Column('operatingCashFlowGrowth', Numeric(12,2)),
         Column('freeCashFlowGrowth', Numeric(12,2)),
         Column('tenYRevenueGrowthPerShare', Numeric(12,2)),
         Column('fiveYRevenueGrowthPerShare', Numeric(12,2)),
         Column('threeYRevenueGrowthPerShare', Numeric(12,2)),
         Column('tenYOperatingGrowthPerShare', Numeric(12,2)),
         Column('fiveYOperatingGrowthPerShare', Numeric(12,2)),
         Column('threeYOperatingGrowthPerShare', Numeric(12,2)),
         Column('tenYNetIncomeGrowthPerShare', Numeric(12,2)),
         Column('fiveYNetIncomeGrowthPerShare', Numeric(12,2)),
         Column('threeYNetIncomeGrowthPerShare', Numeric(12,2)),
         Column('tenYShareholdersEquityGrowthPerShare', Numeric(12,2)),
         Column('fiveYShareholdersEquityGrowthPerShare', Numeric(12,2)),
         Column('threeYShareholdersEquityGrowthPerShare', Numeric(12,2)),
         Column('tenYDividendperShareGrowthPerShare', Numeric(12,2)),
         Column('fiveYDividendperShareEquityGrowthPerShare', Numeric(12,2)),
         Column('threeYDividendperShareEquityGrowthPerShare', Numeric(12,2)),
         Column('receivablesGrowth', Numeric(12,2)),
         Column('inventoryGrowth', Numeric(12,2)),
         Column('assetGrowth', Numeric(12,2)),
         Column('bookValueperShareGrowth', Numeric(12,2)),
         Column('debtGrowth', Numeric(12,2)),
         Column('rdexpenseGrowth', Numeric(12,2)),
         Column('sgaexpensesGrowth', Numeric(12,2)),
         Column('created_on',DateTime(),default = datetime.now),  
         Column('updated_on',DateTime(),default = datetime.now, onupdate=datetime.now)  )

engine = create_engine(address, pool_recycle = 3600)
metadata.create_all(engine) 

names_towrite = symbols.to_dict(orient='records')
price_towrite = price_data.to_dict(orient='records')
ratios_towrite = fin_ratios.to_dict(orient='records')
growth_towrite  = fin_growth.to_dict(orient='records')

names = connection.execute(stock_names.insert(),names_towrite)
prices = connection.execute(price_data.insert(), price_towrite)
ratios = connection.execute(fin_ratio.insert(),ratios_towrite)
growth = connection.execute(fin_growth.insert(),growth_towrite)

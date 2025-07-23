import pandas
import requests
import comtradeapicall
import warnings
import argparse


import os

directory = 'data'
os.makedirs(directory, exist_ok=True)

parser = argparse.ArgumentParser(description='Comtrade API Secret')

parser.add_argument('--subscription_key', type=str, required=True, help='Comtrade API Secret')

args = parser.parse_args()


subscription_key = args.subscription_key

directory = 'data'

#theres a limit on api that you can send only 12 periods so we are looping it
periods = [
    '2000,2001,2002,2003,2004,2005,2006,2007,2008,2009',
    '2010,2011,2012,2013,2014,2015,2016,2017,2018,2019',
    '2020,2021,2022,2023,2024,2025',
]

reporterCodes = [
    '276'
]

cmdCodes = [
    '50'
]


typeCode='C'
freqCode='A'
clCode='HS'
flowCode='m' #m import x export   
partnerCode='0'
partner2Code='0'
customsCode='c00'
motCode=None
fileName="comtrade_data.csv"

dfs = []

for period in periods:
    for reporterCode in reporterCodes:
        for cmdCode in cmdCodes:
            print("Getting for periods:", period, ", reporterCodes:", reporterCode, ", cmdCodes:", cmdCode)
            df = comtradeapicall.getFinalData(subscription_key,
                                              period=period,
                                              typeCode=typeCode, freqCode=freqCode, clCode=clCode, reporterCode=reporterCode, cmdCode=cmdCode, flowCode=flowCode,
                                              partnerCode=partnerCode, partner2Code=partner2Code, customsCode=customsCode, motCode=motCode,
                                              maxRecords=500000, format_output='JSON',
                                              aggregateBy=None, breakdownMode='plus', countOnly=None, includeDesc=True)

            if df is not None and not df.empty:
                dfs.append(df)


with warnings.catch_warnings():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    merged_df = pandas.concat(dfs, ignore_index=True)

csv_path = os.path.join(directory, fileName)
merged_df.to_csv(csv_path, index=False)

print("File saved to:", fileName, "(", len(merged_df), " records written )")

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

def group_by_n(s, n=3):
    items = s.split(',')  # ['1', '2', '3', ..., '6']
    grouped = []
    for i in range(0, len(items), n):
        group = items[i:i+n]
        grouped.append(','.join(group))
    return grouped


periodsStr = '2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025'
reporterCodesStr = '276,50'
cmdCodesStr= '50'
#theres a limit on api that you can send only 12 periods so we are looping it
periods = group_by_n(periodsStr,10)

reporterCodes = group_by_n(reporterCodesStr, 3)
cmdCodes = group_by_n(cmdCodesStr)


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
                print(len(df), " records found")
                dfs.append(df)


with warnings.catch_warnings():
    warnings.simplefilter(action='ignore', category=FutureWarning)
    merged_df = pandas.concat(dfs, ignore_index=True)

csv_path = os.path.join(directory, fileName)
merged_df.to_csv(csv_path, index=False)
print("\n--------------------------------------------\n")
print(f"File saved to: data/{fileName} ({len(merged_df)} records written)")


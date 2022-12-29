import logging

import numpy as np
import pandas as pd
# from Utility.save_file import *
from Reconcilation_Prod_Code.Kingsdaughter.Utility.save_file import*
from pandas import DatetimeIndex

class kingsdaughter_reconcilation_report:
    def Kingsdaughter(self,df):
        try:

            save_file = save_statement_to_output_folder()
            date_sr = pd.to_datetime(pd.Series(df['List Date']))
            df['List Date'] = date_sr.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(df['Last Payment Date']))
            df['Last Payment Date'] = date_sr1.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(df['Cancel Date']))
            df['Cancel Date'] = date_sr1.dt.strftime('%m-%d-%Y')
            df["Last Payment Date.1"] = df["Cancel Description"]
            df.drop(labels=["Cancel Description"], axis="columns", inplace=True)
            df = df.rename(columns={'Last Payment Date.1': 'Cancel Description'}, inplace=False)
            df['Date'] = pd.datetime.now().date()
            # print(df['Date'])
            df['year1'] = pd.DatetimeIndex(df['Date']).year
            df['month1'] = pd.DatetimeIndex(df['Date']).month
            df['day1'] = pd.DatetimeIndex(df['Date']).day
            df['year2'] = pd.DatetimeIndex(df['List Date']).year
            df['month2'] = pd.DatetimeIndex(df['List Date']).month
            df['day2'] = pd.DatetimeIndex(df['List Date']).day
            index_name = df[
                (df['year2'] >= df['year1']) & (df['month2'] >= df['month1']) & (df['day2'] > df['day1'])].index
            df.drop(index_name, inplace=True)
            df.drop('year1', axis=1, inplace=True)
            df.drop('year2', axis=1, inplace=True)
            df.drop('month1', axis=1, inplace=True)
            df.drop('month2', axis=1, inplace=True)
            df.drop('day1', axis=1, inplace=True)
            df.drop('day2', axis=1, inplace=True)
            df.drop('Date', axis=1, inplace=True)
            df2 = df[df.duplicated(['Client #'])].sort_values('Client #')
            # print(df2)
            df4 = df.merge(df2, indicator=True, how='left')
            df4 = df4.sort_values('Client #')
            df4['Disposition'] = pd.to_numeric(df4['Disposition'], errors='coerce')
            df4.loc[((df4['Disposition'] != 9000) & (df4['Disposition'] != 9999)), 'Cancel Code'] = ''
            df4.loc[((df4['Disposition'] != 9000) & (df4['Disposition'] != 9999)), 'Cancel Description'] = ''
            print(df4.to_string())
            df4.drop('_merge', axis=1, inplace=True)
            df2 = df2.reset_index(drop=True)
            df2['Client #'] = df2['Client #'].apply(str)
            df4['Client #'] = df4['Client #'].apply(str)
            df5 = df4[df4['Issue Detected'] == "Account is closed in FACS, but open in KDH File."]
            df6 = df4[df4['Issue Detected'] == "Account is in FACS, but not in KDH File."]
            df7 = df4[(df4['Issue Detected'] == "Account is in KDH File, but it is not in FACS.")]
            df8 = df4[(df4['Issue Detected'] == "The balance in FACS is greater than the balance in KDH File.")]
            df9 = df4[(df4['Issue Detected'] == "The balance in KDH File is greater than the balance in FACS.")]
            frames3 = [df8, df9]
            df10 = pd.concat(frames3)
            df11 = df4[df4['Issue Detected'] == "The balances in FACS and KDH File are same."]



       
            save_file.send_statement(df5, 'Closed in FACS','Kingsdaughter','Kingsdaughter')
            save_file.send_statement(df6, 'FACS not RECON','Kingsdaughter','Kingsdaughter')
            save_file.send_statement(df7, 'RECON not FACS', 'Kingsdaughter', 'Kingsdaughter')
            save_file.send_statement(df10, 'Balance Mismatch','Kingsdaughter','Kingsdaughter')
            save_file.send_statement(df11, 'Balance Match','Kingsdaughter','Kingsdaughter')
        
        except Exception as e:
            logging.exception("error")

    

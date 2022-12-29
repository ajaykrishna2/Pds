
import numpy as np
import pandas as pd

# from Utility.save_file import *
from Reconcilation_Prod_Code.Riverview.Utility.save_file import*

class riverview_reconcilation_report:
    def Riverview(self,df):
        # try:
            save_file = save_statement_to_output_folder()
            pd.set_option('display.float_format', lambda x: '%.0f' % x)
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

            df['year1'] = pd.DatetimeIndex(df['Date']).year
            df['month1'] = pd.DatetimeIndex(df['Date']).month
            df['day1'] = pd.DatetimeIndex(df['Date']).day
            df['year2'] = pd.DatetimeIndex(df['List Date']).year
            df['month2'] = pd.DatetimeIndex(df['List Date']).month
            df['day2'] = pd.DatetimeIndex(df['List Date']).day
            index_name = df[(df['year2'] >= df['year1']) & (df['month2'] >= df['month1']) & (df['day2'] > df['day1'])].index
            df.drop(index_name, inplace=True)
            df.drop('year1', axis=1, inplace=True)
            df.drop('year2', axis=1, inplace=True)
            df.drop('month1', axis=1, inplace=True)
            df.drop('month2', axis=1, inplace=True)
            df.drop('day1', axis=1, inplace=True)
            df.drop('day2', axis=1, inplace=True)
            df.drop('Date', axis=1, inplace=True)
            print(df.style.format({"List Date": lambda t: t.strftime("%d-%m-%Y")}))
            df1 = df[df.duplicated(['EPIC #'], keep=False)].sort_values('EPIC #')
            df2 = df[df.duplicated(['EPIC #'])].sort_values('EPIC #')
            df3 = df1.merge(df2, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df4 = df.merge(df1, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df2["Recon file (EPIC) balance"] = df2["Recon file (EPIC) balance"]
            df4.drop('_merge', axis=1, inplace=True)
            # print(df4.to_string())
            print(len(df4))
            # df2 = df2.reset_index(drop=True)
            # df3 = df3.reset_index(drop=True)
            print(df2.to_string())
            print(df3.to_string())
            df2 = df2.set_index('EPIC #')
            df3 = df3.set_index('EPIC #')
            df2.update(df3["Recon file (EPIC) balance"])
            df2 = df2.reset_index()
            df3 = df3.reset_index()
            # df2["Recon file (EPIC) balance"] = np.where(df2["EPIC #"] == df3["EPIC #"], df3["Recon file (EPIC) balance"])
            # df2["Recon file (EPIC) balance"] = df3["Recon file (EPIC) balance"]
            df2['Match?'] = np.where(df2['Med-1 Balance'] == df2['Recon file (EPIC) balance'], 'True', 'False')
            # create new column in df1 to check if prices match
            df2['Issue Detected'] = df2['Match?'].apply(lambda x: 'Balance Match' if x == 'True' else 'Balance Mismatch')
            df2.drop('Match?', axis=1, inplace=True)
            # print(df4.to_string())
            df2['EPIC #'] = df2['EPIC #'].apply(str)
            df4['EPIC #'] = df4['EPIC #'].apply(str)
            df5 = df4[df4['Issue Detected'] == "Account is closed in FACS, but open in RV File."]
            df6 = df4[df4['Issue Detected'] == "Account is in FACS, but not in River View File."]
            df7 = df4[df4['Issue Detected'] == "Account is in RV File, but it is not in FACS."]
            df8 = df4[(df4['Issue Detected'] == "The balance in FACS is greater than the balance in RV File.")]
            df9 = df4[(df4['Issue Detected'] == "The balance in RV File is greater than the balance in FACS.")]
            df10 = pd.concat([df8, df9])
            df11 = df2[df2['Issue Detected'] == "Balance Mismatch"]
            df12 = pd.concat([df10, df11])
            df13 = df4[df4['Issue Detected'] == "The balances in FACS and RV File are same."]
            df14 = df2[df2['Issue Detected'] == "Balance Match"]
            df15 = pd.concat([df13, df14])

       
            save_file.send_statement(df5, 'Closed in FACS','Riverview','Riverview')
            save_file.send_statement(df6, 'FACS not RECON','Riverview','Riverview')
            save_file.send_statement(df7, 'RECON not FACS','Riverview','Riverview')
            save_file.send_statement(df12, 'Balance Mismatch','Riverview','Riverview')
            save_file.send_statement(df15, 'Balance Match','Riverview','Riverview')
        
    # except Exception as e:
        # logging.exception("error")

    

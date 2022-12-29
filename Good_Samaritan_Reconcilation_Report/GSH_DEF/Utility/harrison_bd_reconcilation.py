import logging
import numpy as np
import pandas as pd
import re
from Utility.save_file import*
from pandas import DatetimeIndex

class harrison_bd_reconcilation_report:
    def Harrison_BD(self,df,inf):
        try:

            save_file = save_statement_to_output_folder()
            pd.set_option('display.float_format', lambda x: '%.0f' % x)
            dfw =df[df.groupby(['CLIENT ACCOUNT #'])['FACS #'].transform('nunique') > 1]
            df.drop(df[df.groupby(['CLIENT ACCOUNT #'])['FACS #'].transform('nunique') > 1].index,inplace=True)
            df.drop('Last Payment Date.1', axis=1, inplace=True)
            pd.set_option('display.float_format', lambda x: '%.0f' % x)
            date_sr = pd.to_datetime(pd.Series(df['List Date']))
            df['List Date'] = date_sr.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(df['Last Payment Date']))
            df['Last Payment Date'] = date_sr1.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(df['Cancel Date']))
            df['Cancel Date'] = date_sr1.dt.strftime('%m-%d-%Y')
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
            df1 = df[df.duplicated(['CLIENT ACCOUNT #'], keep=False)].sort_values('CLIENT ACCOUNT #')
            df2 = df[df.duplicated(['CLIENT ACCOUNT #'])].sort_values('CLIENT ACCOUNT #')
            i=df2.index
            df.drop(i, inplace=True)
            df3 = df1[df1['Recon file (Harrison) balance'].isna()]
            df4= df1[~df1['Recon file (Harrison) balance'].isna()]
            df3 = df3.reset_index(drop=True)
            df4 = df4.reset_index(drop=True)
            df3["Recon file (Harrison) balance"] = df4["Recon file (Harrison) balance"]
            df3['Match?'] = np.where(df3['Med-1 Balance'] == df3['Recon file (Harrison) balance'], 'True', 'False')
            df3['Issue Detected'] = df3['Match?'].apply(lambda x: 'Balance Match' if x == 'True' else 'Balance Mismatch')
            df3.drop('Match?', axis=1, inplace=True)
            df5 = pd.concat([df,df3])
            df5=df5[['Issue Detected','FACS #','CLIENT ACCOUNT #','Client ID','First Name','Last Name','Disposition','Phase','List Date',
                   'Med-1 Balance','Amount Cancelled','Recon file (Harrison) balance','Cancel Code','Cancel Description','Cancel Date',
                   'Last Payment Date','Harrison F/C','Send Dt/Tm']]
            df6 = df5[df5['Issue Detected'] == "Acct is in Harrison file but closed in FACS"]
            df7=df5[df5['Issue Detected'] == "Account is in FACS, but not in the Harrison file"]
            df8=df5[df5['Issue Detected'] == "Account is in Harrison CO File, but it is not in FACS."]
            df9=df5[df5['Issue Detected'] == "The balance in FACS is greater than the bal in the Harrison file"]
            df10=df5[df5['Issue Detected'] == "The balance in the Harrison file is greater than the bal in FACS"]
            df11=df3[df3['Issue Detected'] == "Balance Mismatch"]
            df12=df5[df5['Issue Detected'] == "The balances in FACS and the Harrison file match"]
            df13=df3[df3['Issue Detected'] == "Balance Match"]
            df14=pd.concat([df9, df10])
            df15=pd.concat([df14, df11])
            df16=pd.concat([df12, df13])
            inf_dup=inf[inf.duplicated(['Client REF #'])].sort_values('Client REF #').index
            inf.drop(inf_dup, inplace=True)
            inf['Issue Deteched']=''
            inf['Phase'] = ''
            inf['send dt/tm'] = ''
            inf.drop(labels=['Unnamed: 15','Last Pay DTE','Unnamed: 20'], axis="columns", inplace=True)
            inf = inf.rename(columns={'Unnamed: 17': 'Last Pay DTE', 'Current BAL': 'Med1 BAL'},inplace=False)
            date_sr4 = pd.to_datetime(pd.Series(inf['Last Pay DTE']))
            inf['Last Pay DTE'] = date_sr4.dt.strftime('%m-%d-%Y')
            inf = inf[['Issue Deteched','FACS Acct','Client REF #','Client ID','First Name','Last Name','Disposition','Phase','List DTE',	               'Med1 BAL','AMT Cancelled','Recon File BAL','Cancel Code','Cancel Description','Cancel DTE','Last Pay DTE','Client Name',
                 'send dt/tm']]
            inf = inf.drop(inf[inf['Cancel Code'] == 11].index)
            inf = inf.drop(inf[inf['Cancel Code'] == 71].index)
            inf1 = inf[inf.duplicated(['Client REF #'], keep=False)].sort_values('Client REF #')
            inf=inf.drop(inf1[inf1['Cancel Code'] == 75].index)
            inf2=inf.sort_values(by="List DTE").drop_duplicates(subset=["Client REF #"], keep="last")
            inf2= inf2.rename(columns={'Issue Deteched':'Issue Detected','FACS Acct': 'FACS #', 'Client REF #': 'CLIENT ACCOUNT #',
                  'List DTE': 'List Date','AMT Cancelled':'Amount Cancelled','Med1 BAL':'Med-1 Balance',
                  'Recon File BAL':'Recon file (Harrison) balance','Cancel DTE':'Cancel Date','Last Pay DTE':'Last Payment Date',
                  'send dt/tm':'Send Dt/Tm'}, inplace=False)
            inf2.drop(labels=['Client Name'], axis="columns", inplace=True)
            inf2['CLIENT ACCOUNT #'] = inf2['CLIENT ACCOUNT #'].astype(str)
            inf3 = pd.merge(df8, inf2, on='CLIENT ACCOUNT #')
            inf3=inf3[['Issue Detected_x','CLIENT ACCOUNT #','Harrison F/C','Send Dt/Tm_x','FACS #_y','Client ID_y','First Name_y',
                'Last Name_y','Disposition_y','Phase_y','List Date_y','Med-1 Balance_y','Amount Cancelled_y',
                'Recon file (Harrison) balance_y','Cancel Code_y','Cancel Description_y','Cancel Date_y','Last Payment Date_y']]
            inf3 = inf3.rename(columns={'Issue Detected_x':'Issue Detected',  'Send Dt/Tm_x':'Send Dt/Tm', 'FACS #_y':'FACS #', 
                   'Client ID_y':'Client ID','First Name_y':'First Name', 'Last Name_y':'Last Name', 'Disposition_y':'Disposition', 
                   'Phase_y':'Phase', 'List Date_y':'List Date', 'Med-1 Balance_y':'Med-1 Balance','Amount Cancelled_y':'Amount Cancelled',
                   'Recon file (Harrison) balance_y':'Recon file (Harrison) balance', 'Cancel Code_y':'Cancel Code',
                   'Cancel Description_y': 'Cancel Description','Cancel Date_y':'Cancel Date','Last Payment Date_y':'Last Payment Date'}, 
                    inplace=False)
            inf3=inf3[['Issue Detected', 'FACS #', 'CLIENT ACCOUNT #', 'Client ID', 'First Name', 'Last Name', 'Disposition',
                 'Phase', 'List Date', 'Med-1 Balance', 'Amount Cancelled', 'Recon file (Harrison) balance',
                 'Cancel Code', 'Cancel Description', 'Cancel Date', 'Last Payment Date', 'Harrison F/C', 'Send Dt/Tm']]
            inf4 = inf3[inf3.duplicated(['CLIENT ACCOUNT #'], keep=False)].sort_values('CLIENT ACCOUNT #')
            df18=pd.concat([df6,inf4])
            df19=inf3[((inf3['Disposition'] == '9000')|(inf3['Disposition'] == '9999'))]
            df19["Issue Detected"].replace({"Account is in Harrison CO File, but it is not in FACS.": 
                "Acct is in Harrison file but closed in FACS"}, inplace=True)
            df20=pd.concat([df18,df19])#closed facs
            inf3.drop(inf3[((inf3['Disposition'] == '9000')|(inf3['Disposition'] == '9999'))].index,inplace=True)
            inf3['Match?'] = np.where(inf3['Med-1 Balance'] == inf3['Recon file (Harrison) balance'], 'True', 'False')
            inf3['Issue Detected'] = inf3['Match?'].apply(
                lambda x: 'Balance Match' if x == 'True' else 'Balance Mismatch')
            inf3.drop('Match?', axis=1, inplace=True)
            df21 = inf3[inf3['Issue Detected'] == "Balance Match"]
            df22 = inf3[inf3['Issue Detected'] == "Balance Mismatch"]
            df23 = pd.concat([df16, df21])
            df24 = pd.concat([df15, df22])
            inf3 = inf3.drop(inf3[((inf3['Issue Detected'] == 'Balance Match') | (
                        inf3['Issue Detected'] == 'Balance Mismatch'))].index)
            index_names5 = df7[df7['CLIENT ACCOUNT #'].str.startswith("F", na=False)].index
            df7.drop(index_names5, inplace=True)
            index_names6=df7[df7['Med-1 Balance']==0].index
            df7.drop(index_names6, inplace=True)
            df7 = df7.drop(df7[~((df7['Phase'] == 30) | (df7['Phase'] == 50))].index)
            df7['Cancel Code']=''
            df23['Cancel Code'] = ''
            df24['Cancel Code'] = ''
            df23['Cancel Date']=''
            df24['Cancel Date']=''
            dfw['Cancel Code'] = ''
            df7['Cancel Description'] = ''
            df23['Cancel Description'] = ''
            df24['Cancel Description'] = ''
            dfw['Cancel Description'] = ''
            save_file.send_statement(df20, 'Closed in FACS','HA1_BD_HOSP','Harrison_BD')
            save_file.send_statement(df7, 'FACS not RECON','HA1_BD_HOSP','Harrison_BD')
            save_file.send_statement(inf3, 'RECON not FACS','HA1_BD_HOSP','Harrison_BD')
            save_file.send_statement(df24, 'Balance Mismatch','HA1_BD_HOSP','Harrison_BD')
            save_file.send_statement(df23, 'Balance Match','HA1_BD_HOSP','Harrison_BD')
            save_file.send_statement(dfw, 'Multiples','HA1_BD_HOSP','Harrison_BD')
            
        
        except Exception as e:
            logging.exception("Error in processing harrison_bd recon file")

    

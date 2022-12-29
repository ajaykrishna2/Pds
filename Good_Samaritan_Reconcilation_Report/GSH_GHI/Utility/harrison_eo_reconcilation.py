import logging
import numpy as np
import pandas as pd
import re
from Utility.save_file import*
from pandas import DatetimeIndex

class harrison_eo_reconcilation_report:
    def Harrison_EO(self,df,inf):
        try:
            save_file = save_statement_to_output_folder()
            dfw =df[df.groupby(['CLIENT ACCOUNT #'])['FACS #'].transform('nunique') > 1]
            dfw1 = dfw[dfw['Recon file (Harrison) balance'].isna()]
            dfw2 = dfw[~dfw['Recon file (Harrison) balance'].isna()]
            df.drop(dfw1.index,inplace=True)
            df.drop(dfw2.index, inplace=True)
            df.drop('Last Payment Date.1', axis=1, inplace=True)
            pd.set_option('display.float_format', lambda x: '%.0f' % x)
            date_sr = pd.to_datetime(pd.Series(df['List Date']))
            df['List Date'] = date_sr.dt.strftime('%m-%d-%Y')
            date_sr_inf = pd.to_datetime(pd.Series(inf['List DTE']))
            inf['List DTE'] = date_sr_inf.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(df['Last Payment Date']))
            df['Last Payment Date'] = date_sr1.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(df['Cancel Date']))
            df['Cancel Date'] = date_sr1.dt.strftime('%m-%d-%Y')
            date_sr1 = pd.to_datetime(pd.Series(inf['Cancel DTE']))
            inf['Cancel DTE'] = date_sr1.dt.strftime('%m-%d-%Y')
            df['Date'] = pd.datetime.now().date()
            df['year1'] = pd.DatetimeIndex(df['Date']).year
            df['month1'] = pd.DatetimeIndex(df['Date']).month
            df['day1'] = pd.DatetimeIndex(df['Date']).day
            df['year2'] = pd.DatetimeIndex(df['List Date']).year
            df['month2'] = pd.DatetimeIndex(df['List Date']).month
            df['day2'] = pd.DatetimeIndex(df['List Date']).day
            index_name = df[(df['year2'] >= df['year1']) & (df['month2'] >= df['month1']) & (df['day2'] >= df['day1'])].index
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
            i=df1.index
            df.drop(df1.index, inplace=True)
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
            inf['Issue Deteched']=''
            inf['Phase'] = ''
            inf['send dt/tm'] = ''
            inf.drop(labels=['Unnamed: 15','Last Pay DTE','Unnamed: 20'], axis="columns", inplace=True)
            inf = inf.rename(columns={'Unnamed: 17': 'Last Pay DTE', 'Current BAL': 'Med1 BAL'},inplace=False)
            date_sr4 = pd.to_datetime(pd.Series(inf['Last Pay DTE']))
            inf['Last Pay DTE'] = date_sr4.dt.strftime('%m-%d-%Y')
            date_sr = pd.to_datetime(pd.Series(inf['List DTE']))
            inf['List DTE'] = date_sr.dt.strftime('%d-%m-%Y')
            inf = inf[['Issue Deteched','FACS Acct','Client REF #','Client ID','First Name','Last Name','Disposition','Phase','List DTE',
                       'Med1 BAL','AMT Cancelled','Recon File BAL','Cancel Code','Cancel Description', 'Cancel DTE','Last Pay DTE',
                       'Client Name','send dt/tm']]
            inf2= inf.rename(columns={'Issue Deteched':'Issue Detected','FACS Acct': 'FACS #', 'Client REF #': 'CLIENT ACCOUNT #',
                            'List DTE': 'List Date','AMT Cancelled':'Amount Cancelled','Med1 BAL':'Med-1 Balance',
                            'Recon File BAL':'Recon file (Harrison) balance','Cancel DTE':'Cancel Date','Last Pay DTE':'Last Payment Date',
                            'send dt/tm':'Send Dt/Tm'}, inplace=False)
            inf2.drop(labels=['Client Name'], axis="columns", inplace=True)
            inf2['CLIENT ACCOUNT #'] = inf2['CLIENT ACCOUNT #'].astype(str)
            inf3 = pd.merge(df8,inf2, on='CLIENT ACCOUNT #',how='left')
            inf3=inf3[['Issue Detected_x','CLIENT ACCOUNT #','Recon file (Harrison) balance_x','Harrison F/C','Send Dt/Tm_x','FACS #_y',
                       'Client ID_y','First Name_y','Last Name_y','Disposition_y','Phase_y','List Date_y','Med-1 Balance_y',
                       'Amount Cancelled_y','Recon file (Harrison) balance_y','Cancel Code_y','Cancel Description_y','Cancel Date_y',
                       'Last Payment Date_y']]
            inf3 = inf3.rename(columns={'Issue Detected_x':'Issue Detected','Recon file (Harrison) balance_x':'Recon file (Harrison) balance',                               'Send Dt/Tm_x':'Send Dt/Tm', 'FACS #_y':'FACS #', 'Client ID_y':'Client ID','First Name_y':'First Name', 
                               'Last Name_y':'Last Name', 'Disposition_y':'Disposition', 'Phase_y':'Phase', 'List Date_y':'List Date',
                               'Med-1 Balance_y':'Med-1 Balance','Amount Cancelled_y':'Amount Cancelled',  'Cancel Code_y':'Cancel Code',
                               'Cancel Description_y': 'Cancel Description','Cancel Date_y':'Cancel Date',
                               'Last Payment Date_y':'Last Payment Date'}, inplace=False)
            inf3=inf3[['Issue Detected' ,'FACS #', 'CLIENT ACCOUNT #', 'Client ID', 'First Name', 'Last Name', 'Disposition',
                 'Phase', 'List Date', 'Med-1 Balance', 'Amount Cancelled', 'Recon file (Harrison) balance',
                 'Cancel Code', 'Cancel Description', 'Cancel Date', 'Last Payment Date', 'Harrison F/C', 'Send Dt/Tm']]
            date_sr_inf = pd.to_datetime(pd.Series(inf3['List Date']))
            inf3['List Date'] = date_sr_inf.dt.strftime('%m-%d-%Y')
            inf3.drop(inf3[inf3['Cancel Code'] == 11].index,inplace=True)
            inf4=inf3[inf3.duplicated(['CLIENT ACCOUNT #'], keep=False)].sort_values('CLIENT ACCOUNT #')
            inf3.drop(inf4.index,inplace=True)
            inf5=inf4[((inf4["Cancel Code"] ==71)|(inf4["Cancel Code"] ==75))]
            inf4.drop(inf5.index, inplace=True)
            inf6=inf5[inf5.duplicated(['CLIENT ACCOUNT #'], keep=False)].sort_values('CLIENT ACCOUNT #')
            inf5.drop(inf6.index,inplace=True)
            inf9=inf6[inf6.duplicated(['CLIENT ACCOUNT #'], keep=False)].sort_values('CLIENT ACCOUNT #')
            inf6.drop(inf9.index,inplace=True)
            inf9.drop(inf9[(inf9["Cancel Code"] ==75)].index,inplace=True)
            inf6=pd.concat([inf6,inf9])
            inf6['Date'] = inf6.groupby(['CLIENT ACCOUNT #'])['List Date'].transform('max')
            inf6_dt = pd.to_datetime(pd.Series(inf6['Date']))
            inf6['Date'] = inf6_dt.dt.strftime('%m-%d-%Y')
            inf6['Match?'] = np.where(inf6['List Date'] == inf6['Date'], 'True', 'False')
            inf6_index = inf6[inf6['Match?'] == 'False'].index
            date_sr_inf = pd.to_datetime(pd.Series(inf6['List Date']))
            inf6['List Date'] = date_sr_inf.dt.strftime('%m-%d-%Y')
            inf6.drop('Match?', axis=1, inplace=True)
            inf6.drop('Date', axis=1, inplace=True)
            inf6.drop(inf6_index, inplace=True)
            inf7=pd.concat([inf4,inf5])
            inf8=inf7[inf7.duplicated(['CLIENT ACCOUNT #'], keep=False)].sort_values('CLIENT ACCOUNT #')
            index71=inf8[(inf8["Cancel Code"] == 71)].index
            index75 = inf8[(inf8["Cancel Code"] == 75)].index
            inf8.drop(index71,inplace=True)
            inf8.drop(index75,inplace=True)
            date_sr_inf = pd.to_datetime(pd.Series(inf8['List Date']))
            inf8['List Date'] = date_sr_inf.dt.strftime('%m-%d-%Y')
            inf3=pd.concat([inf3,inf6,inf8])
            df19=inf3[((inf3['Disposition'] == '9000')|(inf3['Disposition'] == '9999'))]
            df19["Issue Detected"].replace({"Account is in Harrison CO File, but it is not in FACS.":
                                    "Acct is in Harrison file but closed in FACS"}, inplace=True)
            df20=pd.concat([df6,df19])#closed facs
            inf3.drop(inf3[((inf3['Disposition'] == '9000')|(inf3['Disposition'] == '9999'))].index,inplace=True)
            df_r = inf3[inf3['Med-1 Balance'].isna()]
            inf3.drop(inf3[(inf3['Med-1 Balance'].isna())].index,inplace=True)
            inf3['Match?'] = np.where(inf3['Med-1 Balance'] == inf3['Recon file (Harrison) balance'], 'True', 'False')
            inf3['Issue Detected'] = inf3['Match?'].apply(lambda x: 'Balance Match' if x == 'True' else 'Balance Mismatch')
            inf3.drop('Match?', axis=1, inplace=True)
            df21 = inf3[inf3['Issue Detected'] == "Balance Match"]
            df22 = inf3[inf3['Issue Detected'] == "Balance Mismatch"]
            df23 = pd.concat([df16, df21])
            df24 = pd.concat([df15, df22])
            inf3.drop(inf3[((inf3['Issue Detected'] == 'Balance Match') | (
                        inf3['Issue Detected'] == 'Balance Mismatch'))].index)
            index_names5 = df7[df7['CLIENT ACCOUNT #'].str.startswith("F", na=False)].index
            df7.drop(index_names5, inplace=True)
            index_names6=df7[df7['Med-1 Balance']==0].index
            df7.drop(index_names6, inplace=True)
            df7.drop(df7[~(df7['Phase'] == 10) ].index,inplace=True)
            df20.drop(df20[(df20['Cancel Code'] == 75)].index, inplace=True)
            df20.drop(df20[(df20['Cancel Code'] == 11)].index, inplace=True)
            df25 = df20[(df20['Recon file (Harrison) balance'] < 0) & ((df20['Cancel Code'].isna()))]
            dfm25 = df20[(df20['Recon file (Harrison) balance'] < 0) & ((df20['Cancel Code'] == 2))]
            df25["Issue Detected"].replace({"Acct is in Harrison file but closed in FACS": "Balance Mismatch"},
                                           inplace=True)
            dfm25["Issue Detected"].replace({"Acct is in Harrison file but closed in FACS": "Balance Mismatch"},
                                            inplace=True)
            df26 = pd.concat([df24, df25, dfm25])
            df20.drop(df25.index, inplace=True)
            df20.drop(dfm25.index, inplace=True)
            df23.reset_index(inplace=True)
            df23.drop('index', axis=1, inplace=True)
            df23.drop(df23[((df23['Client ID'] == '220ERP') & (df23['Disposition'] == '2LFA'))].index, inplace=True)
            df7['Cancel Code']=''
            df23['Cancel Code'] = ''
            df26['Cancel Code'] = ''
            dfw1['Cancel Code'] = ''
            df7['Cancel Description'] = ''
            df23['Cancel Description'] = ''
            df26['Cancel Description'] = ''
            dfw1['Cancel Description'] = ''
            df7['Cancel Date'] = ''
            df23['Cancel Date'] = ''
            df26['Cancel Date'] = ''       
            save_file.send_statement(df20, 'Closed in FACS','HA2_EO_HOSP','Harrison_EO')
            save_file.send_statement(df7, 'FACS not RECON','HA2_EO_HOSP','Harrison_EO')
            save_file.send_statement(df_r, 'RECON not FACS','HA2_EO_HOSP','Harrison_EO')
            save_file.send_statement(df26, 'Balance Mismatch','HA2_EO_HOSP','Harrison_EO')
            save_file.send_statement(df23, 'Balance Match','HA2_EO_HOSP','Harrison_EO')
            save_file.send_statement(dfw1, 'Multiples','HA2_EO_HOSP','Harrison_EO')
    
        
        except Exception as e:
            logging.exception("Error in processing harrison_eo recon file")

    


import numpy as np
import pandas as pd
import xlsxwriter
# from Utility.save_file import *
from Reconcilation_Prod_Code.Womenshospital.Utility.save_file import*

class womenshospital_reconcilation_report:
    def Womenshospital(self,df,inf):
        # try:
            save_file = save_statement_to_output_folder()
            df.drop(['Last Payment Date.1', 'Days in Dispo'], axis=1, inplace=True)
            df['Cancel Description'] = np.NAN
            df = df[['Issue Detected', 'FACS #', 'EPIC #', 'Client ID', 'First Name', 'Last Name', 'Disposition', 'Phase',
                 'List Date', 'Med-1 Balance', 'Recon file (EPIC) balance', 'Amount Cancelled', 'Last Payment Date','Cancel Code',
                 'Cancel Description', 'Epic Type']]
            dfw = df[df.groupby(['EPIC #'])['FACS #'].transform('nunique') > 1]
            dfw = dfw.sort_values(by=['EPIC #','FACS #'], ascending=[True,True],na_position='first')
            dfw['Recon file (EPIC) balance'] = dfw['Recon file (EPIC) balance'].replace(to_replace=[np.NAN,0], method='ffill')
            df.drop(dfw.index, inplace=True)
            dfw.drop(dfw[(dfw['FACS #'].isna())].index, inplace=True)
            pd.set_option('display.float_format', lambda x: '%.0f' % x)
            date_sr = pd.to_datetime(pd.Series(df['List Date']))
            df['List Date'] = date_sr.dt.strftime('%m-%d-%Y')

            date_sr1 = pd.to_datetime(pd.Series(df['Last Payment Date']))
            df['Last Payment Date'] = date_sr1.dt.strftime('%m-%d-%Y')
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

            print(len(df))
            df1 = df[df.duplicated(['EPIC #'], keep=False)].sort_values('EPIC #')
            print(len(df1))
            df2 = df[df.duplicated(['EPIC #'])].sort_values('EPIC #')
            print(len(df2))
            # df2.drop(df2[(df2['FACS #'].isna())].index, inplace=True)
            df3 = df1.merge(df2, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            df = df.merge(df1, indicator=True, how='left').loc[lambda x: x['_merge'] != 'both']
            print(len(df))
            df2 = df2.reset_index()
            df3 = df3.reset_index()
            df2["Recon file (EPIC) balance"] = df3["Recon file (EPIC) balance"]
            df2['Match?'] = np.where(df2['Med-1 Balance'] == df2['Recon file (EPIC) balance'], 'True', 'False')
            df2['Issue Detected'] = df2['Match?'].apply(lambda x: 'Balance Match' if x == 'True' else 'Balance Mismatch')
            df2.drop('Match?', axis=1, inplace=True)
            df4 = df[df['Issue Detected'] == "Account is closed in FACS, but open in EPIC."]
            df5=df[df['Issue Detected'] =="Account is in EPIC, but it is either not in FACS, or does not have EPIC flag."]
            df6 = df[df['Issue Detected'] == "Account is in FACS, but not in TWH EPIC."]
            df7 = df[df['Issue Detected'] == "The balance in EPIC is greater than the balance in FACS."]
            df8 = df[df['Issue Detected'] == "The balance in FACS is greater than the balance in EPIC."]
            df9 = df2[df2['Issue Detected'] == "Balance Mismatch"]
            df10=pd.concat([df7,df8,df9])
            df11=df[df['Issue Detected'] == "Balance in EPIC and balance in FACS match."]
            df12 = df2[df2['Issue Detected'] == "Balance Match"]
            df13=pd.concat([df11,df12])
            print(df5.head(10).to_string())
            print(inf)
            print(inf.head(10).to_string())
            inf.drop(labels=['Unnamed: 15', 'Unnamed: 17', 'Unnamed: 20'], axis="columns", inplace=True)
            date_sr = pd.to_datetime(pd.Series(inf['List DTE']))
            inf['List DTE'] = date_sr.dt.strftime('%d-%m-%Y')
            inf['Issue Deteched'] = ''
            inf['Phase'] = ''
            inf['Payment Date'] = ''
            inf['Days in Dispo'] = np.NAN
            inf['PMT TO PRN'] = np.NAN

            inf = inf[['Issue Deteched', 'FACS Acct', 'Client REF #', 'Client ID', 'First Name', 'Last Name', 'Disposition',
                 'Phase', 'List DTE', 'Current BAL', 'Recon File BAL', 'AMT Cancelled', 'Cancel Description', 'Cancel Code',
                 'Last Pay DTE', 'Days in Dispo','Disp Chang DTE', 'Unnamed: 16']]
            inf2 = inf.rename(
                columns={'Issue Deteched': 'Issue Detected', 'FACS Acct': 'FACS #', 'Client REF #': 'EPIC #',
                         'List DTE': 'List Date', 'AMT Cancelled': 'Amount Cancelled', 'Current BAL': 'Med-1 Balance',
                         'Recon File BAL': 'Recon file (EPIC) balance', 'Cancel DTE': 'Cancel Date',
                         'Last Pay DTE': 'Last Payment Date','Disp Chang DTE': 'Last Payment Date.1','Unnamed: 16':'Epic Type'}, inplace=False)
            inf2 = inf2[['Issue Detected', 'FACS #', 'EPIC #', 'Client ID', 'First Name', 'Last Name', 'Disposition', 'Phase',
                 'List Date', 'Med-1 Balance', 'Recon file (EPIC) balance', 'Amount Cancelled', 'Last Payment Date',
                 'Cancel Code','Cancel Description' ,'Epic Type']]
            inf2['Cancel Code'] = pd.to_numeric(inf2['Cancel Code'], errors='coerce').astype(float)
            # print(inf2.head(10).to_string())
            index11 = inf2[(inf2['Cancel Code'] == 71) | (inf2['Cancel Code'] == 11)].index
            print(index11)
            inf2.drop(index11, inplace=True)
            print(df5)
            inf3= inf2[inf2.duplicated(['EPIC #'])].sort_values('EPIC #')
            inf2.drop(inf3.index,inplace=True)
            inf3=pd.concat([inf2,inf3])
            print(df5.head(2).to_string())
            print(inf3.head(2).to_string())

            inf3 = pd.merge(inf3,df5,on='EPIC #',how='outer')
            print(inf3.info())
            # st = inf3[inf3['Disposition_x'].str.contains('5', regex=True)]
            # print(inf3['Disposition_y'])
            inf3335 = inf3[(inf3['FACS #_x'].isna())]
            inf3.drop(inf3335[(inf3335['FACS #_x'].isna())].index, inplace=True)
            inf3335 = inf3335.rename(columns={'Issue Detected_y': 'Issue Detected','Recon file (EPIC) balance_y': 'Recon file (EPIC) balance','FACS #_y': 'FACS #', 'Epic Type_y': 'Epic Type','Client ID_y': 'Client ID','First Name_y': 'First Name', 'Last Name_y': 'Last Name','Disposition_y': 'Disposition', 'Phase_y': 'Phase', 'List Date_y': 'List Date','Med-1 Balance_y': 'Med-1 Balance','Amount Cancelled_y': 'Amount Cancelled','Cancel Description_y': 'Cancel Description','Cancel Code_y': 'Cancel Code','Last Payment Date_y': 'Last Payment Date','Days in Dispo_y': 'Days in Dispo','Cancel Date_y': 'Cancel Date', 'PMT TO PRN_y': 'PMT TO PRN'},inplace=False)
            inf3335 = inf3335[['Issue Detected', 'FACS #', 'EPIC #', 'Client ID', 'First Name', 'Last Name', 'Disposition', 'Phase',
                 'List Date', 'Med-1 Balance', 'Recon file (EPIC) balance', 'Amount Cancelled', 'Last Payment Date',
                 'Cancel Code', 'Cancel Description','Epic Type']]

            inf3 = inf3.rename(columns={'Issue Detected_y': 'Issue Detected','Recon file (EPIC) balance_x': 'Recon file (EPIC) balance','FACS #_x': 'FACS #', 'Epic Type_x':'Epic Type','Client ID_x': 'Client ID', 'First Name_x': 'First Name','Last Name_x': 'Last Name', 'Disposition_x': 'Disposition', 'Phase_x': 'Phase','List Date_x': 'List Date', 'Med-1 Balance_x': 'Med-1 Balance','Amount Cancelled_x': 'Amount Cancelled', 'Cancel Description_x': 'Cancel Description','Cancel Code_x': 'Cancel Code', 'Last Payment Date_y': 'Last Payment Date','Days in Dispo_y': 'Days in Dispo', 'Cancel Date_x': 'Cancel Date','PMT TO PRN_y': 'PMT TO PRN'}, inplace=False)
            print(inf3.info())


            inf3 = inf3[['Issue Detected', 'FACS #', 'EPIC #', 'Client ID', 'First Name', 'Last Name', 'Disposition', 'Phase',
                 'List Date', 'Med-1 Balance', 'Recon file (EPIC) balance', 'Amount Cancelled','Last Payment Date',
                 'Cancel Code','Cancel Description',  'Epic Type']]
            infff_sr = pd.to_datetime(pd.Series(inf3['List Date']))
            inf3['List Date'] = infff_sr.dt.strftime('%m-%d-%Y')

            # inf333 = inf3[(inf3['FACS #'].isna())]
            print(inf3335)
            print(inf3['Disposition'])

            inf33 = inf3[inf3.duplicated(['EPIC #'])].sort_values('EPIC #')

            inf3.drop(inf33.index,inplace=True)
            print(inf33)

            inf3.drop(inf33[(inf33['FACS #'].isna())].index, inplace=True)
            print(inf33)
            print(inf3['Issue Detected'])
            print(inf33['Issue Detected'])
            inf3=pd.concat([inf3,inf33])
            print(inf3.head(2).to_string())
            inf34=inf3
            print(inf34)
            legal = inf34.loc[inf34['Disposition'].str.startswith('5', na=False)]
            inf34.drop(legal.index,inplace=True)
            cf=inf34[(inf34['Disposition']=='9000')|(inf34['Disposition']=='9999')]
            print(cf)
            cf["Issue Detected"].replace({"Account is in EPIC, but it is either not in FACS, or does not have EPIC flag.  ": "Account is closed in FACS, but open in EPIC."},inplace=True)

            # cf.drop('index', axis=1, inplace=True)
            df4.drop('_merge', axis=1, inplace=True)
            # cf.reset_index(inplace=True)
            # df4.reset_index(inplace=True)
            print(cf.columns)
            print(df4.columns)
            df4=pd.concat([df4,cf])
            inf34.drop(cf.index, inplace=True)
            inf34['Match?'] = np.where(inf34['Med-1 Balance'] == inf34['Recon file (EPIC) balance'], 'True', 'False')
            inf34['Issue Detected'] = inf34['Match?'].apply(lambda x: 'Balance Match' if x == 'True' else 'Balance Mismatch')
            inf34.drop('Match?', axis=1, inplace=True)
            df14 = inf34[inf34['Issue Detected'] == "Balance Mismatch"]
            df14['Match?'] = np.where(df14['Med-1 Balance'] - df14['Recon file (EPIC) balance'] > 0, 'True', 'False')
            df14['Issue Detected'] = df14['Match?'].apply(lambda x: 'The balance in FACS is greater than the balance in EPIC.' if x == 'True' else 'The balance in EPIC is greater than the balance in FACS.')
            df14.drop('Match?', axis=1, inplace=True)
            print(df14)
            df15= inf34[inf34['Issue Detected'] == "Balance Match"]
            df16=pd.concat([df10,df14])
            df17=pd.concat([df13,df15])
            findex=df6[df6['Med-1 Balance']==0].index
            df6.drop(findex,inplace=True)
            df16.drop('index', axis=1, inplace=True)
            df17.drop('index', axis=1, inplace=True)
            df6.drop('_merge', axis=1, inplace=True)
            df16.drop('_merge', axis=1, inplace=True)
            df17.drop('_merge', axis=1, inplace=True)
            legal['Cancel Code']=''
            legal['Cancel Description']=''
            df6['Cancel Code'] = ''
            df6['Cancel Description'] = ''
            df16['Cancel Code'] = ''
            df16['Cancel Description'] = ''
            df17['Cancel Code'] = ''
            df17['Cancel Description'] = ''
            

       
            save_file.send_statement(df4,'Closed in FACS','TWH_RECON','Rec_Womens_Hospital')
            save_file.send_statement(inf3335, 'RECON not FACS', 'TWH_RECON', 'Rec_Womens_Hospital')
            save_file.send_statement(df6, 'FACS not RECON', 'TWH_RECON', 'Rec_Womens_Hospital')
            save_file.send_statement(legal, 'Legal', 'TWH_RECON', 'Rec_Womens_Hospital')
            save_file.send_statement(df16, 'Balance Mismatch','TWH_RECON','Rec_Womens_Hospital')
            save_file.send_statement(df17, 'Balance Match','TWH_RECON','Rec_Womens_Hospital')
            save_file.send_statement(dfw, 'Multiples','TWH_RECON','Rec_Womens_Hospital')





    # except Exception as e:
        # logging.exception("error")

    

import glob2
import shutil
import pandas as pd
import os
import boto3
import datetime
import calendar
import configparser

configuartion_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) + "/config/config.ini"
print(configuartion_path)

config = configparser.ConfigParser()
config.read(configuartion_path);
aws_access_key = config['Aws_Credential']['s3_access_key'];
aws_secret_key = config['Aws_Credential']['s3_secret_key'];
bucket_region  = config['Aws_Credential']['bucket_region'];
bucket_name = config['Aws_Credential']['bucket_name'];
reconcilation_input = config['Aws_Credential']['reconcilation_input'];
reconcilation_output = config['Aws_Credential']['reconcilation_output'];
local_directory=config['Aws_Credential']['local_directory'];
#local_output_directory = config['Log_Location']['output_path']



class save_statement_to_output_folder:
    def save_file_to_s3(self):
        if ((datetime.datetime.now().month) != 1):
            year = datetime.datetime.now().year
            month = (datetime.datetime.now().month) - 1
            month_name = calendar.month_name[month]

        else:
            year = datetime.datetime.now().year - 1
            month = (datetime.datetime.now().month) - 1
            month_name = calendar.month_name[month]
        session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                region_name=bucket_region);
        s3 = session.resource("s3")
        my_bucket = s3.Bucket(bucket_name)
        i = local_directory + "Rec_Harrison_Hospital"
        output_filename = i.split('/')[::-1][0]
        directory_contents = os.listdir(i)
        for j in directory_contents:
            path_s3 = reconcilation_output + "/" + "Rec_Harrison_Hospital" + "/" + str(year) + "/" + month_name + "/" + j
            my_bucket.upload_file(i + "/" + j, path_s3)

        os.remove(local_directory + "Rec_Harrison_Hospital/HA1_BD_HOSP.xlsx")

    def send_statement(self, statement_file, sheet_name, file_name, folder_name):
        print("send_statement")
        session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,region_name=bucket_region);
        s3 = session.client('s3');

        file_name = file_name + ".xlsx"
        
        if not os.path.exists(os.path.join(local_directory + folder_name, file_name)):
            writer = pd.ExcelWriter(os.path.join(local_directory + folder_name, file_name),engine='openpyxl')
            worksheet1 = writer.sheets['Closed in FACS']
            worksheet2 = writer.sheets['FACS not RECON']
            worksheet3 = writer.sheets['RECON not FACS']
            worksheet4 = writer.sheets['Multiples']
            worksheet5 = writer.sheets['Balance Mismatch']
            worksheet6 = writer.sheets['Balance Match']
            worksheet1.freeze_panes(1, 0)
            worksheet2.freeze_panes(1, 0)
            worksheet3.freeze_panes(1, 0)
            worksheet4.freeze_panes(1, 0)
            worksheet5.freeze_panes(1, 0)
            worksheet6.freeze_panes(1, 0)
            statement_file.to_excel(writer, sheet_name=sheet_name, index=False)

            writer.save()
        else:
            writer = pd.ExcelWriter(os.path.join(local_directory+ folder_name, file_name), engine='openpyxl', mode='a')
            worksheet1 = writer.sheets['Closed in FACS']
            worksheet2 = writer.sheets['FACS not RECON']
            worksheet3 = writer.sheets['RECON not FACS']
            worksheet4 = writer.sheets['Multiples']
            worksheet5 = writer.sheets['Balance Mismatch']
            worksheet6 = writer.sheets['Balance Match']
            worksheet1.freeze_panes(1, 0)
            worksheet2.freeze_panes(1, 0)
            worksheet3.freeze_panes(1, 0)
            worksheet4.freeze_panes(1, 0)
            worksheet5.freeze_panes(1, 0)
            worksheet6.freeze_panes(1, 0)
            statement_file.to_excel(writer, sheet_name=sheet_name, index=False)
            writer.save()

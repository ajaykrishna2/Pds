import pandas as pd
import logging, boto3, io,datetime
import calendar
import os
import configparser
configuartion_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) + "/config/config.ini"
config = configparser.ConfigParser()
config.read(configuartion_path);
aws_access_key = config['Aws_Credential']['s3_access_key'];
aws_secret_key = config['Aws_Credential']['s3_secret_key'];
bucket_region  = config['Aws_Credential']['bucket_region'];
bucket_name = config['Aws_Credential']['bucket_name'];
reconcilation_input = config['Aws_Credential']['reconcilation_input'];
reconcilation_output = config['Aws_Credential']['reconcilation_output'];

class read:
    if((datetime.datetime.now().month)!=1):
            year  = datetime.datetime.now().year
            month = (datetime.datetime.now().month) -1
            month_name = calendar.month_name[month]

    else:
            year = datetime.datetime.now().year -1
            month = (datetime.datetime.now().month) - 1
            month_name = calendar.month_name[month]
    
    def collect_and_preprocess_harrison_eo_data(self):
        try:
            session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
            region_name=bucket_region);
            s3 = session.resource('s3')
            list_of_files = []
            my_bucket = s3.Bucket(bucket_name)
            #Listing of  all files within bucket
            for file in my_bucket.objects.all():
               list_of_files.append(file.key)
            filtered_list =list( filter(lambda x:'Reconcilation_Input' in x,list_of_files))
            for i in filtered_list:
              if 'Rec_Harrison_Hospital' in i and str(read.year) in i and read.month_name in i and 'HA2_EO_Incoming' in i:
                client = boto3.client('s3',aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret_key,
                region_name=bucket_region)
                obj = client.get_object(Bucket=bucket_name, Key=i)
                data = obj['Body'].read()
                harrison_eo_reconcilation_statement = pd.read_table(io.BytesIO(data),sep="\t",index_col=False, encoding="ISO-8859-1")
            return harrison_eo_reconcilation_statement


        except Exception as e:
            logging.exception("error in reading harrison_eo_reconcilation file")

    def info_out_read(self):
        try:
            session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
            region_name=bucket_region);
            s3 = session.resource('s3')
            list_of_files = []
            my_bucket = s3.Bucket(bucket_name)
            #Listing of  all files within bucket
            for file in my_bucket.objects.all():
               list_of_files.append(file.key)
            filtered_list =list( filter(lambda x:'Reconcilation_Input' in x,list_of_files))
            for i in filtered_list:
              if 'Rec_Harrison_Hospital' in i and str(read.year) in i and read.month_name in i and 'Info-out_EO' in i:
                client = boto3.client('s3',aws_access_key_id= aws_access_key, aws_secret_access_key=aws_secret_key,
                region_name=bucket_region)
                obj = client.get_object(Bucket=bucket_name, Key=i)
                data = obj['Body'].read()
                info_out_read = pd.read_table(io.BytesIO(data),sep="|",index_col=False, encoding="ISO-8859-1")
            return info_out_read


        except Exception as e:
            logging.exception("error in reading harrison_info_eo_reconcilation file")

            


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

class read_input_for_reconcilation:
    if((datetime.datetime.now().month)!=1):
            year  = datetime.datetime.now().year
            month = (datetime.datetime.now().month) -1
            month_name = calendar.month_name[month]

    else:
            year = datetime.datetime.now().year -1
            month = (datetime.datetime.now().month) - 1
            month_name = calendar.month_name[month]

    def collect_and_preprocess_community_data(self):
        try:
            session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,region_name=bucket_region);
            cli=session.client('s3')
            data = cli.get_object(Bucket=bucket_name, Key=reconcilation_input+"/Rec_Community/"+str(read_input_for_reconcilation.year)+"/"+read_input_for_reconcilation.month_name+"/"+"COMMUNITY_EPIC_RECON_041221_61206.TXT")
            data = data['Body'].read()
            harrison_reconcilation_statement = pd.read_table(io.BytesIO(data),sep="\t",index_col=False, encoding="ISO-8859-1")
            print(harrison_reconcilation_statement )

            logging.info("read Community_reconcilation_statement file successfully")
            return harrison_reconcilation_statement

        except Exception as e:
            logging.exception("error in reading file")

    



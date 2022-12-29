import pandas as pd
import logging, boto3, io,datetime
import calendar
import os
import configparser

configuartion_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + "/config/config.ini"
print(configuartion_path)

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
            month = (datetime.datetime.now().month) -2
            month_name = calendar.month_name[month]

    else:
            year = datetime.datetime.now().year -1
            month = (datetime.datetime.now().month) - 2
            month_name = calendar.month_name[month]
    @classmethod
    def collect_and_preprocess_goshen_data(cls):
        try:
            session = boto3.Session(aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                    region_name=bucket_region);
            cli = session.client('s3')
            data = cli.get_object(Bucket=bucket_name, Key=reconcilation_input + "/Rec_Goshen_Hospital/" + str(cls.year) + "/" + cls.month_name + "/" + "Goshen-N_Recon_100621_8797.TXT")
            data = data['Body'].read()
            #data = "/home/ubuntu/RVH_Inven_RECON_033021_39880.TXT"
            riverview_reconcilation_statement = pd.read_table(io.BytesIO(data),sep="\t",index_col=False, encoding="ISO-8859-1")
            

            logging.info("read Goshen_reconcilation_statement file successfully")
            return riverview_reconcilation_statement

        except Exception as e:
            logging.exception("error in reading file")


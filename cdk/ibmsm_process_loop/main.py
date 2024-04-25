from datetime import date
from json import dumps
from os import path, getcwd, listdir
from os.path import join
import csv
import math
import shutil
import sys
import time

from colorama import Fore, Style
from sqlalchemy.orm import sessionmaker
import boto3
import colorama

if 'sftp_files' not in listdir():
    sys.path.append(path.join(getcwd(), 'sftp'))
    
if 'ibmsm' not in listdir():
    sys.path.append(path.join(getcwd(), 'sqlalchemy'))


from sftp_files import sftp_connect
from database import engine, IbmsmProcessLog, Base, Security, ibmsm_csv_mapping

def convert_size(size_bytes:int ) -> str:
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def get_first_unprocessed_ibmsm_record() -> IbmsmProcessLog|None:
    Session = sessionmaker(bind=engine)
    with Session() as session:
        ibmsm_record: IbmsmProcessLog|None = session.query(IbmsmProcessLog).filter(IbmsmProcessLog.process_date.is_(None)).first()
    return ibmsm_record

def set_record_as_processed(filename:str) -> None:
    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.query(IbmsmProcessLog).filter(IbmsmProcessLog.filename==filename).update({'process_date': date.today()})
        session.commit()
        
def download_file(filename, ibmsm_record: IbmsmProcessLog) -> None:
    with sftp_connect() as sftp:
        sftp_file_instance = sftp.open(filename, 'r')
        print(f'reading  {filename} {convert_size(sftp.stat(filename).st_size)}\n')
        buffer_size = 1024 * 1024  # 1 MB
        with open(ibmsm_record.filename, 'wb') as out_file:
            shutil.copyfileobj(sftp_file_instance, out_file, buffer_size)
        
def file_as_model(filename:str) -> list[Security]:
    with open(filename, mode='r', encoding='ISO-8859-1') as file:
        csv_reader = csv.DictReader(file)
        data_list: list[Security] = []
        for row in csv_reader:
            security_map = {
                ibmsm_csv_mapping.get(item[0], item[0]): item[1].strip() 
                for item in row.items() 
                if ibmsm_csv_mapping.get(item[0]) is not None
            }
            data_list.append(Security(**security_map))    

def add_to_db(security_list: list[Security]):
    Session = sessionmaker(bind=engine)
    with Session() as session:
        session.bulk_save_objects(security_list)
        session.commit()
        
def publish_event() -> str:
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']    
    sns_client = boto3.client('sns')
    response = sns_client.publish(
    TopicArn=sns_topic_arn,
    Message=dumps({
        'default': dumps({
            'message': 'Hello from ME!!!'
        }),
        'sms': 'Hello from ME!!!',
        'email': 'Hello from ME!!!'
    }),
    MessageStructure='json'
    )
    return response 

def print_green_time_event(time_object: time, text_:str, width=20) -> None:
    formatted_text = f"{text_:{width}}"
    print(f"{Fore.GREEN}{formatted_text}{Style.RESET_ALL}{round(time.time()-time_object, 3)}s")
    time_object = time.time()
    
def handler(event, context):
    colorama.init(autoreset=True)
    last_time = time.time()
    ibmsm_record = get_first_unprocessed_ibmsm_record()
    print_green_time_event(last_time, 'got record')
    folder_name:str = "/users/ETFCM/RPT"

    if ibmsm_record:
        filename = join(folder_name, ibmsm_record.filename)
        
        download_file(filename, ibmsm_record)
        print_green_time_event(last_time, 'downloaded file')

        security_list: list[Security] = file_as_model(ibmsm_record.filename)
        print_green_time_event(last_time, 'translated to model')

        add_to_db(security_list)
        print_green_time_event(last_time, 'commited to db')
   
        set_record_as_processed(filename)
        publish_event()
        print_green_time_event(last_time, 'moving on...')

    else:
        print(f"{Fore.CYAN}    DONE    {Style.RESET_ALL}")
   

        
if __name__ == "__main__":
    Security.__table__.create(bind=engine, checkfirst=True)
    handler(None, None)
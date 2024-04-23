from datetime import datetime, timedelta
import json
import os
import boto3
import sys
from os import path, getcwd

from pysftp import Connection
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if 'sftp_files' not in os.listdir():
    sys.path.append(path.join(getcwd(), 'sftp'))
    
if 'ibmsm' not in os.listdir():
    sys.path.append(path.join(getcwd(), 'sqlalchemy'))

from sftp_files import sftp_connect
from database import engine, IbmsmProcessLog, Base

def filter_old_and_non_ibmsm_files(
    sftp: Connection, folder_name:str, time_difference: timedelta = timedelta(weeks=1)
    ) -> dict[str, int]:
    dir_contents = sftp.listdir_attr(folder_name)
    current_time: datetime = datetime.now()
    one_week_delta: timedelta = time_difference

    new_ibmsm: dict[str, int] = {}
    for file_attr in dir_contents:
        file_name = file_attr.filename
        if 'ibmsm' in file_name.lower():
            file_mtime = datetime.fromtimestamp(file_attr.st_mtime)
            if current_time - file_mtime > one_week_delta:
                new_ibmsm[file_name] =file_attr.st_mtime
    return new_ibmsm

def handler(a, b):
    folder_name:str = "/users/ETFCM/RPT"
    
    process_interval: timedelta = timedelta(weeks=1)
    sftp: Connection = sftp_connect()
    files: dict[str, int] = filter_old_and_non_ibmsm_files(sftp, folder_name, process_interval)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        files_that_exist_already: list[tuple[str]] = session.query(IbmsmProcessLog.filename).filter(IbmsmProcessLog.filename.in_(files.keys())).all()
    
    for filename in files_that_exist_already:
        if filename[0] in files:
            del files[filename[0]]
        
    new_files: list[IbmsmProcessLog] = [IbmsmProcessLog(filename=filename, mtime=datetime.fromtimestamp(mtime)) for filename, mtime in files.items()]
    with Session() as session:
        session.bulk_save_objects(new_files)
        session.commit()
    sftp.close()
    
    sns_topic_arn = os.environ['SNS_TOPIC_ARN']    
    sns_client = boto3.client('sns')
    response = sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps({
            'default': json.dumps({
                'message': 'Hello from IbmsmCronChecker Lambda!'
            }),
            'sms': 'Hello from IbmsmCronChecker Lambda!',
            'email': 'Hello from IbmsmCronChecker Lambda!'
        }),
        MessageStructure='json'
    )
    print(f'response, {response}')
    
if __name__ == "__main__":
    IbmsmProcessLog.__table__.create(bind=engine, checkfirst=True)
    handler(None, None)
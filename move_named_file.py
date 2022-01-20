from ensurepip import version
import os
from glob import glob
import prefect
import datetime
from prefect import task, Flow, Parameter
from prefect.tasks.shell import ShellTask
from prefect.storage import GitHub
import paramiko
import json
from prefect.run_configs import KubernetesRun
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

FLOW_NAME = "move_named_file"
STORAGE = GitHub(
    repo="radbrt/prefect_k8s",
    path=f"move_named_file.py"
)


def get_kv_secret(secretname):
    kvurl = 'https://bricksvault.vault.azure.net/'
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url = kvurl, credential = credential)

    s = client.get_secret(secretname)
    return s.value




@task()
def transfer_named_file(FTP_CREDS_SECRET, source_file, target_file, encoding, version_file=True):

    os.makedirs(f"/data/", exist_ok=True)
    os.makedirs(f"/converted/", exist_ok=True)

    if version_file:
        original_filename = os.path.basename(source_file)
        ext = original_filename.split('.')[-1]
        base = '.'.join(original_filename.split('.')[:-1])
        versionstamp = str(datetime.datetime.utcnow().strftime('%s'))
        filename = (base or 'file') + '.' + versionstamp + '.' + ext
    else:
        filename = os.path.basename(source_file)

    logger = prefect.context.get('logger')
    logger.info(f"source_file: {source_file}")
    logger.info(f"target_file: {target_file}")

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ftpcreds = json.loads(get_kv_secret(FTP_CREDS_SECRET))
    FTP_HOST = ftpcreds['HOST']
    FTP_USERNAME = ftpcreds['USERNAME']
    FTP_PASSWORD = ftpcreds['PASSWORD']
    logger.info(f"Connecting to {FTP_HOST}")

    ssh_client.connect(
        hostname=FTP_HOST,
        username=FTP_USERNAME, 
        password=FTP_PASSWORD
    )

    ftp_client = ssh_client.open_sftp()


    ftp_client.get(f"{source_file}", f"/data/{filename}")

    converttask = ShellTask(
        command=f"iconv -f {encoding} -t utf-8 /data/{filename} > /converted/{filename}"
    ).run()

    put_file_gcs.run(f"/converted/{filename}", target_file)


@task
def put_file_gcs(source_file, target_file):

    credstring = get_kv_secret('GCP-KEY')
    cred = json.loads(credstring)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred)
    client = storage.Client(credentials=credentials, project='radjobads')

    bucket = client.get_bucket('radjobads')

    blob = bucket.blob(target_file)
    blob.upload_from_filename(source_file)


with Flow(FLOW_NAME, storage=STORAGE, 
    run_config=KubernetesRun(
        labels=["aks"], image='radbrt.azurecr.io/prefectaz')) as flow:

    FTP_CREDS_SECRET = Parameter('FTP_CREDS_SECRET_NAME')
    SOURCE_FILE = Parameter('SOURCE_FILE')
    regex = Parameter('regex', default='.*')
    encoding = Parameter('encoding', default='utf-8')
    TARGET_FILE = Parameter('TARGET_FILE')
    
    transfer_named_file(FTP_CREDS_SECRET, SOURCE_FILE, TARGET_FILE, encoding=encoding)

import os
from glob import glob
import prefect
from prefect import task, Flow
from prefect.tasks.shell import ShellTask
from prefect.storage import GitHub
import paramiko
import json
from prefect.run_configs import KubernetesRun
from gcloud import storage
from oauth2client.service_account import ServiceAccountCredentials

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

FLOW_NAME = "move_ftp_files"
STORAGE = GitHub(
    repo="radbrt/prefect_k8s",
    path=f"movefile_flow.py"
)

def get_kv_secret(secretname):
    kvurl = 'https://bricksvault.vault.azure.net/'
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url = kvurl, credential = credential)

    s = client.get_secret(secretname)
    return s

@task
def get_ftp_files(pathname, regex):

    logger = prefect.context.get('logger')
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh_client.connect(
        hostname=get_kv_secret('FTP-HOST'), 
        username=get_kv_secret('FTP-USER'), 
        password=get_kv_secret('FTP-PWD')
    )

    commandinput, commandoutput, commanderror = ssh_client.exec_command(
        command=f"find ~/{pathname} -type f -regex '{regex}'"
    )

    findlist = commandoutput.readlines()
    logger.info(findlist)

    os.makedirs(f"/data/{pathname}", exist_ok=True)
    os.makedirs(f"/converted/{pathname}/", exist_ok=True)

    ftp_client = ssh_client.open_sftp()

    for file in findlist:
        cleanfile = file.strip()
        basename = cleanfile.split('/')[-1]
        logger.info(cleanfile)
        logger.info(basename)

        ftp_client.get(cleanfile, f"/data/{pathname}/{basename}")

        converttask = ShellTask(
            command=f"iconv -t utf-8 /data/{pathname}/{basename} > /converted/{pathname}/{basename}"
        ).run()

        put_file_gcs.run(f"/converted/{pathname}/{basename}")

    ftp_client.close()


@task
def put_file_gcs(file_location):
    credstring = get_kv_secret('GCP-KEY')
    cred = json.loads(credstring)

    credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred)
    client = storage.Client(credentials=credentials, project='radjobads')
    bucket = client.get_bucket('radjobads')

    blob = bucket.blob(file_location[1:])
    blob.upload_from_filename(file_location)


with Flow(FLOW_NAME, storage=STORAGE, 
    run_config=KubernetesRun(
        labels=["aks"], image='radbrt.azurecr.io/prefectaz')) as flow:

    get_ftp_files('two_types', '.*eventA.*\.csv')

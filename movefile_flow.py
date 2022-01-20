import os
from glob import glob
import prefect
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
    return s.value

def get_gcp_filenames(bucket_prefix):

    credstring = get_kv_secret('GCP-KEY')
    cred = json.loads(credstring)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred)
    client = storage.Client(credentials=credentials, project='radjobads')

    bucket = client.get_bucket('radjobads')

    filenames = [file.name.split('/')[-1].strip() for file in bucket.list_blobs(prefix=bucket_prefix)]

    return filenames


@task
def get_new_ftp_files(KV_CONNECT_SECRET_NAME, SOURCE_PATH, regex, TARGET_PATH='misc', encoding='utf-8'):

    logger = prefect.context.get('logger')
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ftpcreds = json.loads(get_kv_secret(KV_CONNECT_SECRET_NAME))
    FTP_HOST = ftpcreds['HOST']
    FTP_USERNAME = ftpcreds['USERNAME']
    FTP_PASSWORD = ftpcreds['PASSWORD']
    logger.info(f"Connecting to {FTP_HOST}")

    ssh_client.connect(
        hostname=FTP_HOST,
        username=FTP_USERNAME, 
        password=FTP_PASSWORD
    )

    commandinput, commandoutput, commanderror = ssh_client.exec_command(
        command=f"find {SOURCE_PATH} -type f -maxdepth 1 -regex '{regex}'"
    )

    findlist = commandoutput.readlines()
    logger.info(f"All files: {findlist}")

    os.makedirs(f"/data/", exist_ok=True)
    os.makedirs(f"/converted/", exist_ok=True)

    ftp_client = ssh_client.open_sftp()

    existing_files = get_gcp_filenames(TARGET_PATH)
    findlist_filenames = [n.split('/')[-1].strip() for n in findlist]
    logger.info(f"All files: {findlist_filenames}")
    logger.info(f"Existing files: {existing_files}")
    new_files = set(findlist_filenames) - set(existing_files)
    
    logger.info(f"New files: {new_files}")

    for file in new_files:

        deletetask = ShellTask(command=f"rm -f /data/{file}").run()

        ftp_client.get(f"{SOURCE_PATH}/{file}", f"/data/{file}")

        if encoding.lower() != 'utf-8':
            converttask = ShellTask(
                helper_script=f"rm -f /converted/{file}",
                command=f"iconv -f {encoding} -t utf-8 /data/{file} > /converted/{file}"
            ).run()
            logger.info(f"finished converting")
        else:
            movetask = ShellTask(
                helper_script=f"rm -f /converted/{file}",
                command=f"mv /data/{file} /converted/{file}"
            ).run()
            logger.info(f"excepting, move file instead")

        put_file_gcs.run(f"/converted/{file}", f"{TARGET_PATH}/{file}")

    ftp_client.close()

@task()
def put_file_ssh(source_path, target_path, SSH_CREDS):
    pass

@task()
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
    SOURCE_PATH = Parameter('SOURCE_PATH')
    regex = Parameter('regex', default='.*')
    encoding = Parameter('encoding', default='utf-8')
    TARGET_PATH = Parameter('TARGET_PATH', default='misc')
    
    get_new_ftp_files(FTP_CREDS_SECRET, SOURCE_PATH, regex, encoding=encoding, TARGET_PATH=TARGET_PATH)
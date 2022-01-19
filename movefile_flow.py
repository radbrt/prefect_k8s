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

@task
def get_ftp_files(KV_CONNECT_SECRET_NAME, pathname, regex, file_nick='default', encoding='UTF-8'):

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
        command=f"find ~/{pathname} -type f -regex '{regex}'"
    )

    findlist = commandoutput.readlines()
    logger.info(findlist)

    os.makedirs(f"/data/{pathname}/{file_nick}", exist_ok=True)
    os.makedirs(f"/converted/{pathname}/{file_nick}", exist_ok=True)

    ftp_client = ssh_client.open_sftp()

    for file in findlist:
        cleanfile = file.strip()
        basename = cleanfile.split('/')[-1]
        logger.info(cleanfile)
        logger.info(basename)

        deletetask = ShellTask(command=f"rm -f /converted/{pathname}/{file_nick}/{basename}").run()
        logger.info(f"Deleted: {deletetask}")
        ftp_client.get(cleanfile, f"/data/{pathname}/{file_nick}/{basename}")
        logger.info(f"FTP Done")

        try:
            converttask = ShellTask(
                helper_script=f"rm -f /converted/{pathname}/{file_nick}/{basename}",
                command=f"iconv -f {encoding} -t utf-8 /data/{pathname}/{file_nick}/{basename} > /converted/{pathname}/{file_nick}/{basename}"
            ).run()
            logger.info(f"finished converting")
        except Exception as e:
            movetask = ShellTask(
                helper_script=f"rm -f /converted/{pathname}/{file_nick}/{basename}",
                command=f"mv /data/{pathname}/{file_nick}/{basename} /converted/{pathname}/{file_nick}/{basename}"
            ).run()
            logger.info(f"excepting, move file instead")

        logger.info(f"Putting to /converted/{pathname}/{file_nick}/{basename}")
        put_file_gcs.run(f"/converted/{pathname}/{file_nick}/{basename}")

    ftp_client.close()


@task
def put_file_gcs(file_location):

    logger = prefect.context.get('logger')
    logger.info(file_location)
    logger.info(f"Is this a file? {os.path.isfile(file_location}")
    credstring = get_kv_secret('GCP-KEY')
    cred = json.loads(credstring)
    logger.info('creds acquired')
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred)
    client = storage.Client(credentials=credentials, project='radjobads')
    
    logger.info('client created')
    bucket = client.get_bucket('radjobads')
    logger.info('bucket fetched')
    blob = bucket.blob(file_location[1:])
    logger.info('blob fetched')
    blob.upload_from_filename(file_location)
    logger.info('upload done')

with Flow(FLOW_NAME, storage=STORAGE, 
    run_config=KubernetesRun(
        labels=["aks"], image='radbrt.azurecr.io/prefectaz')) as flow:

    FTP_CREDS_SECRET = Parameter('FTP_CREDS_SECRET_NAME')
    PATHNAME = Parameter('pathname')
    regex = Parameter('regex', default='.*')
    encoding = Parameter('encoding', default='utf-8')
    file_nick = Parameter('file_nick', default='default')
    
    get_ftp_files(FTP_CREDS_SECRET, PATHNAME, regex, encoding=encoding, file_nick=file_nick)
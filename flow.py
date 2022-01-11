from prefect import Flow, task
from prefect.storage import GitHub
import prefect
from prefect.run_configs import KubernetesRun
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

FLOW_NAME = "gh_storage_k8s_run"
STORAGE = GitHub(
    repo="radbrt/prefect_k8s",
    path=f"flow.py"
)


@task(log_stdout=True)
def hello_world():
    text = f"hello from {FLOW_NAME}"
    print(text)
    return text

@task
def kv_demo():
    logger = prefect.context.get('logger')
    kvurl = 'https://bricksvault.vault.azure.net/'
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url = kvurl, credential = credential)

    s = client.get_secret('helloworldsecret')
    logger.info(f"The secret is {s.value}")

with Flow(
    FLOW_NAME, storage=STORAGE, run_config=KubernetesRun(labels=["aks"], image='radbrt.azurecr.io/prefect'),
) as flow:
    hw = hello_world()
    kvd = kv_demo()
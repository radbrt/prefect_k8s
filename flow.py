from prefect import Flow, task
from prefect.storage import GitHub
import prefect
from prefect.run_configs import LocalRun

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
    logger.info(f"The secret is well hidden")

with Flow(
    FLOW_NAME, storage=STORAGE, run_config=LocalRun(labels=["aks"]),
) as flow:
    hw = hello_world()
    kvd = kv_demo()
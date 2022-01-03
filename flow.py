from prefect import Flow, task
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun


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


with Flow(
    FLOW_NAME, storage=STORAGE, run_config=KubernetesRun(labels=["k8s"],),
) as flow:
    hw = hello_world()

from airflow import DAG
from typing import Dict, Any
from .models import CreateEMRClusterDAG, EmrAddStepsDAG, EchoStepDAG, EchoClusterDAG


class DAGFactory:
    _dag_models = {
        "create_emr_cluster": CreateEMRClusterDAG,
        "add_emr_step": EmrAddStepsDAG,
        "create_echo_cluster": EchoClusterDAG,
        "echo_step": EchoStepDAG,
    }

    @staticmethod
    def build_dag(job_type: str, dag_definition: Dict[str, Any]):
        creator = DAGFactory._dag_models.get(job_type)
        if not creator:
            raise ValueError(f"Unsupported job type: {job_type}")
        return creator(**dag_definition)

from typing import List, Dict, Any
from .factory import DAGFactory


class DAGBuilder:
    def __init__(self, dag_definitions: List[Dict[str, Any]]):
        self.dag_definitions = dag_definitions

    def build_dags(self):
        dags: List[Dict[str, Any]] = []

        for dag_definition in self.dag_definitions:
            job_info = dag_definition["job_info"]
            job_type = job_info["job_type"]
            dag_id = job_info["job_name"]
            dag = DAGFactory.build_dag(job_type, dag_definition)
            dags.append({"dag_id": dag_id, "dag": dag.build()})

        for dag in dags:
            globals()[dag["dag_id"]] = dag["dag"]

        return dags

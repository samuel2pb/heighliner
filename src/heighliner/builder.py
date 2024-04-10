# from typing import List, Dict, Any
# from .models import CreateEMRClusterDAG, EmrAddStepsDAG, EchoStepDAG, EchoClusterDAG


# class DAGBuilder:
#     def __init__(self, dag_definitions: List[Dict[str, Any]]):
#         self.dag_definitions = dag_definitions

#         self.emr_add_steps_dags = []
#         self.emr_create_cluster_dags = []
#         self.echo_add_steps_dags = []
#         self.echo_create_cluster_dags = []

#         for dag_definition in self.dag_definitions:
#             job_info = dag_definition["job_info"]
#             job_type = job_info["job_type"]

#             if job_type == "create_emr_cluster":
#                 self.emr_create_cluster_dags.append(dag_definition)
#             elif job_type == "add_emr_step":
#                 self.emr_add_steps_dags.append(dag_definition)
#             elif job_type == "create_echo_cluster":
#                 self.echo_create_cluster_dags.append(dag_definition)
#             elif job_type == "echo_step":
#                 self.echo_add_steps_dags.append(dag_definition)

#     def build_dags(self):

#         dags: List[Dict[str, Any]] = []

#         for dag_definition in self.emr_add_steps_dags:
#             job_info = dag_definition["job_info"]
#             dag_id = job_info["job_name"]
#             dag = EmrAddStepsDAG(**dag_definition)
#             dags.append({"dag_id": dag_id, "dag": dag.build()})

#         for dag_definition in self.emr_create_cluster_dags:
#             job_info = dag_definition["job_info"]
#             dag_id = job_info["job_name"]
#             dag = CreateEMRClusterDAG(**dag_definition)
#             dags.append({"dag_id": dag_id, "dag": dag.build()})

#         for dag_definition in self.echo_add_steps_dags:
#             job_info = dag_definition["job_info"]
#             dag_id = job_info["job_name"]
#             dag = EchoStepDAG(**dag_definition)
#             dags.append({"dag_id": dag_id, "dag": dag.build()})

#         for dag_definition in self.echo_create_cluster_dags:
#             job_info = dag_definition["job_info"]
#             dag_id = job_info["job_name"]
#             dag = EchoClusterDAG(**dag_definition)
#             dags.append({"dag_id": dag_id, "dag": dag.build()})

#         for dag in dags:
#             dag_id = dag["dag_id"]
#             dag = dag["dag"]
#             globals()[dag_id] = dag

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

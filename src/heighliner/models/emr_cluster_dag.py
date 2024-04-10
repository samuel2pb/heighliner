from airflow import DAG
from typing import Dict
from ..interfaces.generic_dag import JOBStyleGenericDAG
from ..registry import Registry


registry = Registry().get_registry()


class CreateEMRClusterDAG(JOBStyleGenericDAG):

    signature = "create_emr_cluster"

    def load_values(self):

        self.module_name: str = self.job_info.get("module_name")
        self.dag_id: str = f"dag_{self.module_name}_job_group"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:

        self.load_values()

        from airflow.providers.amazon.aws.operators.emr import (
            EmrCreateJobFlowOperator,
            EmrTerminateJobFlowOperator,
        )
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator
        from ..constants import JOB_FLOW_OVERRIDES

        jobs = registry.get(self.module_name).get("jobs")

        create_cluster_task_id = f"create_cluster_{self.module_name}"
        module_job_ids = []

        for job in jobs:
            job_name = job.get("job_name")
            job_type = job.get("job_type")
            if job_type not in ["create_emr_cluster", "create_echo_cluster"]:
                module_job_ids.append(job_name)

        remove_cluster_task_id = f"remove_cluster_task_{self.module_name}"

        with DAG(
            dag_id=self.dag_id,
            default_args=self.default_args,
            description=self.description,
            tags=self.tags,
            catchup=False,
            max_active_runs=1,
            concurrency=1,
            schedule=self.schedule,
        ) as dag:
            """ """

            create_cluster = EmrCreateJobFlowOperator(
                task_id=create_cluster_task_id,
                job_flow_overrides=JOB_FLOW_OVERRIDES,
                wait_for_completion=True,
                waiter_delay=300,
                waiter_max_attempts=5,
                deferrable=True,
            )

            subdags: Dict[str, TriggerDagRunOperator] = {}

            for job in module_job_ids:
                subdags[job] = TriggerDagRunOperator(
                    task_id=f"trigger_{job}",
                    trigger_dag_id=f"dag_{job}",
                    execution_date="{{ execution_date }}",
                    wait_for_completion=True,
                    poke_interval=60,
                    deferrable=True,
                )

            remove_cluster = EmrTerminateJobFlowOperator(
                task_id=remove_cluster_task_id,
                job_flow_id=create_cluster.output,
                waiter_delay=120,
                waiter_max_attempts=5,
                deferrable=True,
            )

            for job in module_job_ids:
                create_cluster >> subdags[job] >> remove_cluster

            return dag

from pydantic import BaseModel
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union
from .registry import Registry
from airflow import DAG

registry = Registry().get_registry()


class GenericDAG(ABC, BaseModel):
    default_args: Dict[str, Any]
    tags: list
    description: str
    dag_id: str = None
    schedule: Union[str, None] = None

    @abstractmethod
    def validate(self):
        pass

    @abstractmethod
    def build(self):
        pass


class JOBStyleGenericDAG(GenericDAG):
    job_info: Dict[str, Any]
    job_name: str = None
    module_name: str = None


class EchoStepDAG(JOBStyleGenericDAG):

    signature = "echo_step"

    def load_values(self):
        self.job_name: str = self.job_info.get("job_name")
        self.module_name: str = self.job_info.get("module_name")
        self.dag_id: str = f"dag_{self.job_name}"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:
        self.load_values()

        from airflow.operators.empty import EmptyOperator

        start_task_id = f"start_task_{self.job_name}"
        end_task_id = f"end_task_{self.job_name}"

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

            start = EmptyOperator(task_id=start_task_id)
            end = EmptyOperator(task_id=end_task_id)

            start >> end

            return dag


class CreateEchoCluster(JOBStyleGenericDAG):

    signature = "create_echo_cluster"

    def load_values(self):
        self.job_name: str = self.job_info.get("job_name")
        self.module_name: str = self.job_info.get("module_name")
        self.dag_id: str = f"dag_{self.job_name}"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:
        self.load_values()

        from airflow.operators.empty import EmptyOperator
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

        create_cluster_task_id = f"create_cluster_{self.module_name}"  # Precisa ser o valor do xcom dessa task
        module_subdags_ids = []
        jobs = registry.get(self.module_name).get("jobs")

        for job in jobs:
            job_name = job.get("job_name")
            job_type = job.get("job_type")
            if job_type not in ["create_emr_cluster", "create_echo_cluster"]:
                module_subdags_ids.append(f"dag_{job_name}")

        remove_cluster_task_id = f"remove_cluster_task_{self.job_name}"

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

            start = EmptyOperator(task_id=create_cluster_task_id)
            end = EmptyOperator(task_id=remove_cluster_task_id)
            subdags: List[TriggerDagRunOperator] = []
            for subdag in module_subdags_ids:
                subdags.append(
                    TriggerDagRunOperator(
                        task_id=f"trigger_{subdag}",
                        trigger_dag_id=subdag,
                        wait_for_completion=True,
                        poke_interval=60,
                        deferrable=True,
                    )
                )

            start >> subdags >> end

            return dag


class CreateEMRClusterDAG(JOBStyleGenericDAG):

    signature = "create_emr_cluster"

    def load_values(self):

        self.job_name: str = self.job_info.get("job_name")
        self.module_name: str = self.job_info.get("module_name")
        self.dag_id: str = f"{self.module_name}_EMR_MODULE_BUS"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:

        self.load_values()

        from airflow.operators.empty import EmptyOperator
        from airflow.providers.amazon.aws.operators.emr import (
            EmrCreateJobFlowOperator,
            EmrTerminateJobFlowOperator,
        )
        from airflow.utils.task_group import TaskGroup
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator
        from heighliner.constants import JOB_FLOW_OVERRIDES

        create_cluster_task_id = f"create_cluster_{self.module_name}"  # Precisa ser o valor do xcom dessa task
        module_subdags_ids = []
        jobs = registry.get(self.module_name).get("jobs")

        for job in jobs:
            job_name = job.get("job_name")
            job_type = job.get("job_type")
            if job_type not in ["create_emr_cluster", "create_echo_cluster"]:
                module_subdags_ids.append(f"dag_{job_name}")

        remove_cluster_task_id = f"remove_cluster_task_{self.job_name}"

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

            end = EmptyOperator(task_id="end")

            create_cluster = EmrCreateJobFlowOperator(
                task_id=create_cluster_task_id,
                job_flow_overrides=JOB_FLOW_OVERRIDES,
                wait_for_completion=True,
                waiter_delay=300,
                waiter_max_attempts=5,
                deferrable=True,
            )

            subdags: List[TriggerDagRunOperator] = []
            for subdag in module_subdags_ids:
                subdags.append(
                    TriggerDagRunOperator(
                        task_id=f"trigger_{subdag}",
                        trigger_dag_id=subdag,
                        wait_for_completion=True,
                        poke_interval=60,
                        deferrable=True,
                    )
                )

            remove_cluster = EmrTerminateJobFlowOperator(
                task_id=remove_cluster_task_id,
                job_flow_id=create_cluster.output,
                waiter_delay=120,
                waiter_max_attempts=5,
                deferrable=True,
            )

            create_cluster >> subdags >> remove_cluster >> end

            return dag


class EmrAddStepsDAG(JOBStyleGenericDAG):

    signature = "add_emr_step"

    def load_values(self):

        self.job_name: str = self.job_info.get("job_name")
        self.module_name: str = self.job_info.get("module_name")
        self.dag_id: str = f"dag_{self.job_name}"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:

        self.load_values()
        # self.validate()

        from airflow.operators.empty import EmptyOperator
        from airflow.operators.python import PythonOperator, BranchPythonOperator
        from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
        from .utils import build_spark_submit, validate_execution

        validation_task_id = f"validation_task_{self.job_name}"
        get_spark_submit_cmd_task_id = f"get_spark_submit_cmd_task_{self.job_name}"
        add_steps_task_id = f"add_steps_task_{self.job_name}"
        end_task_id = f"end_task_{self.job_name}"

        emr_bus_cluster_id = f"{self.module_name}_EMR_MODULE_BUS"
        emr_bus_cluster_task_id = f"create_cluster_{self.module_name}"
        xcom_pull_statement = "{{ task_instance.xcom_pull(dag_id=${EMR_BUS_DAG}, task_ids=${EMR_CREATE_BUS_TASK})}}".replace(
            "${EMR_BUS_DAG}", emr_bus_cluster_id
        ).replace(
            "${EMR_CREATE_BUS_TASK}", emr_bus_cluster_task_id
        )

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

            branch_from_validation = BranchPythonOperator(
                task_id=validation_task_id,
                python_callable=validate_execution,
                provide_context=True,
            )

            get_spark_submit_cmd = PythonOperator(
                task_id=get_spark_submit_cmd_task_id,
                python_callable=build_spark_submit,
                provide_context=True,
            )

            add_steps = EmrAddStepsOperator(
                task_id=add_steps_task_id,
                job_flow_id=xcom_pull_statement,
                steps=get_spark_submit_cmd.output,
                wait_for_completion=True,
                waiter_delay=60,
                waiter_max_attempts=5,
                deferrable=True,
            )

            end = EmptyOperator(task_id=end_task_id)

            branch_from_validation >> [get_spark_submit_cmd, end]
            get_spark_submit_cmd >> add_steps >> end

            return dag

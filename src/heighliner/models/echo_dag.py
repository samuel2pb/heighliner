from airflow import DAG
from airflow.models.xcom import XCom
from time import sleep
from typing import Dict
from ..interfaces.generic_dag import JOBStyleGenericDAG
from ..registry import Registry

registry = Registry().get_registry()


def get_cmd():
    return "spark-submit --deploy-mode cluster --driver-memory 4g main.py"


def create_echo_cluster():
    sleep(5)
    return "j-89098098098"


def get_cluster(ti, dag_id, task_id):

    value = XCom.get_one(
        execution_date=ti.execution_date,
        key="return_value",
        task_id=task_id,
        dag_id=dag_id,
    )

    return value


def add_step_cmd(ti, cluster_id, spark_submit_cmd):

    print(
        f"running {spark_submit_cmd} on cluster {cluster_id} on logical_date { ti.execution_date }"
    )


class EchoClusterDAG(JOBStyleGenericDAG):

    signature = "create_echo_cluster"

    def load_values(self):
        self.module_name: str = self.job_info.get("module_name")
        self.dag_id: str = f"dag_{self.module_name}_job_group"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:
        self.load_values()

        from airflow.operators.empty import EmptyOperator
        from airflow.operators.python import PythonOperator
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
            subdag: Dict[str, TriggerDagRunOperator] = {}

            create_cluster = PythonOperator(
                task_id=create_cluster_task_id,
                python_callable=create_echo_cluster,
            )

            for job in module_job_ids:
                subdag[job] = TriggerDagRunOperator(
                    task_id=f"trigger_{job}",
                    trigger_dag_id=f"dag_{job}",
                    execution_date="{{ execution_date }}",
                    wait_for_completion=True,
                )

            end = EmptyOperator(task_id=remove_cluster_task_id)

            for job in module_job_ids:
                create_cluster >> subdag[job] >> end

            return dag


class EchoStepDAG(JOBStyleGenericDAG):

    signature = "echo_step"

    def load_values(self):
        self.module_name: str = self.job_info.get("module_name")
        self.job_name: str = self.job_info.get("job_name")
        self.dag_id: str = f"dag_{self.job_name}"

    def validate(self, model):
        return self.model_validate(model)

    def build(self) -> DAG:
        self.load_values()

        from airflow.operators.empty import EmptyOperator
        from airflow.operators.python import PythonOperator
        from airflow.sensors.external_task import ExternalTaskSensor

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
            submit_cmd_task_id = f"get_spark_submit_cmd_{self.job_name}"
            get_cluster_id_task_id = f"get_cluster_id_{self.job_name}"
            add_step_task_id = f"add_step_{self.job_name}"
            end_task_id = f"end_{self.job_name}"

            wait_cluster = ExternalTaskSensor(
                task_id=f"wait_cluster_{self.module_name}",
                external_dag_id=f"dag_{self.module_name}_job_group",
                external_task_id=f"create_cluster_{self.module_name}",
                poll_interval=30,
            )

            get_cluster_id = PythonOperator(
                task_id=get_cluster_id_task_id,
                python_callable=get_cluster,
                op_kwargs={
                    "dag_id": f"dag_{self.module_name}_job_group",
                    "task_id": f"create_cluster_{self.module_name}",
                },
            )

            submit_cmd = PythonOperator(
                task_id=submit_cmd_task_id,
                python_callable=get_cmd,
            )

            add_step = PythonOperator(
                task_id=add_step_task_id,
                python_callable=add_step_cmd,
                op_kwargs={
                    "cluster_id": get_cluster_id.output,
                    "spark_submit_cmd": submit_cmd.output,
                },
            )

            end = EmptyOperator(task_id=end_task_id)

            wait_cluster >> get_cluster_id >> submit_cmd >> add_step >> end

            return dag

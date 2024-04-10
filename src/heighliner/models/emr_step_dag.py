from airflow import DAG
from typing import List
from ..interfaces.generic_dag import JOBStyleGenericDAG
from ..registry import Registry

registry = Registry().get_registry()


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
        from airflow.sensors.external_task import ExternalTaskSensor
        from airflow.operators.python import PythonOperator, BranchPythonOperator
        from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
        from ..utils import get_cluster, build_spark_submit, validate_execution

        wait_cluster_sensor_id = f"wait_cluster_{self.module_name}"
        create_cluster_dag_id = f"dag_{self.module_name}_job_group"
        create_cluster_task_id = f"create_cluster_{self.module_name}"

        validation_task_id = f"validation_task_{self.job_name}"
        get_submit_cmd_task_id = f"get_spark_submit_cmd_{self.job_name}"
        get_cluster_id_task_id = f"get_cluster_id_{self.job_name}"
        add_step_task_id = f"add_step_{self.job_name}"
        end_task_id = f"end_{self.job_name}"

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
            )

            wait_cluster = ExternalTaskSensor(
                task_id=wait_cluster_sensor_id,
                external_dag_id=create_cluster_dag_id,
                external_task_id=create_cluster_task_id,
                poll_interval=30,
            )

            get_cluster_id = PythonOperator(
                task_id=get_cluster_id_task_id,
                python_callable=get_cluster,
                op_kwargs={
                    "dag_id": create_cluster_dag_id,
                    "task_id": create_cluster_task_id,
                },
            )

            get_spark_submit_cmd = PythonOperator(
                task_id=get_submit_cmd_task_id,
                python_callable=build_spark_submit,
            )

            add_step = EmrAddStepsOperator(
                task_id=add_step_task_id,
                job_flow_id=get_cluster_id.output,
                steps=get_spark_submit_cmd.output,
                wait_for_completion=True,
                waiter_delay=60,
                waiter_max_attempts=5,
                deferrable=True,
            )

            end = EmptyOperator(task_id=end_task_id)

            branch_from_validation >> [wait_cluster, end]
            wait_cluster >> get_cluster_id >> get_spark_submit_cmd >> add_step >> end

            return dag

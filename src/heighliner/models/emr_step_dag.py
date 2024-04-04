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
        from airflow.operators.python import PythonOperator, BranchPythonOperator
        from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
        from ..utils import build_spark_submit, validate_execution

        validation_task_id = f"validation_task_{self.job_name}"
        get_spark_submit_cmd_task_id = f"get_spark_submit_cmd_task_{self.job_name}"
        add_steps_task_id = f"add_steps_task_{self.job_name}"
        end_task_id = f"end_task_{self.job_name}"

        emr_bus_cluster_id = f"{self.module_name}_EMR_MODULE_BUS"
        emr_bus_cluster_task_id = f"create_cluster_{self.module_name}"
        xcom_pull_statement = f"{{{{ task_instance.xcom_pull(dag_id='{emr_bus_cluster_id}', task_ids='{emr_bus_cluster_task_id}') }}}}"

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

from airflow.models.xcom import XCom


def get_cluster(ti, dag_id, task_id):

    value = XCom.get_one(
        execution_date=ti.execution_date,
        key="return_value",
        task_id=task_id,
        dag_id=dag_id,
    )

    return value

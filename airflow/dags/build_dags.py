# 'airflow'
from typing import List, Dict, Any
from heighliner.utils import load_yaml
from heighliner.builder import DAGBuilder
from heighliner.registry import Registry

registry = Registry().get_registry()

dag_definitions: List[Dict[str, Any]] = []

for module_name, module_info in registry.items():
    for job in module_info["jobs"]:
        path = job["path"]
        dag_definitions.append(load_yaml(path))

DAGBuilder(dag_definitions).build_dags()

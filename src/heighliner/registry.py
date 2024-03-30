import os
from typing import Dict, Any, List
from .types import RegistryType
from .utils import load_yaml  # Verify ciclyc import


AIRFLOW_YAML_FILES_PATH = os.environ.get("AIRFLOW_YAML_FILES_PATH", "/opt/airflow/yaml")


class SingletonMeta(type):
    """
    A Singleton metaclass that creates a single instance of a class.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Registry(metaclass=SingletonMeta):
    def __init__(self):
        self.registry = {}

    def load_yaml_files(self):
        """
        Load YAML files and update the registry with job information under
        the corresponding module_name. The job information includes job_name
        and the path to the YAML file.
        """
        for file in os.listdir(AIRFLOW_YAML_FILES_PATH):
            if file.endswith(".yaml") or file.endswith(".yml"):
                file_path = os.path.join(AIRFLOW_YAML_FILES_PATH, file)
                # with open(file_path, "r") as f:
                #     yaml_data = yaml.safe_load(f)
                yaml_data = load_yaml(file_path)
                if "job_info" in yaml_data:
                    module_name = yaml_data["job_info"].get("module_name")
                    job_name = yaml_data["job_info"].get("job_name")
                    job_type = yaml_data["job_info"].get("job_type")
                    if module_name and job_name:
                        if module_name not in self.registry:
                            self.registry[module_name] = {"jobs": []}
                        self.registry[module_name]["jobs"].append(
                            {
                                "job_name": job_name,
                                "path": file_path,
                                "job_type": job_type,
                            }
                        )

    def get_registry(self) -> RegistryType:
        """
        Return the current state of the registry.
        """
        return self.registry

from pydantic import BaseModel
from abc import ABC, abstractmethod
from typing import Any, Dict, Union


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

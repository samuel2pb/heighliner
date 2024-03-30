from typing import Dict, List, TypedDict


class Job(TypedDict):
    job_name: str
    path: str


class ModuleJobs(TypedDict):
    jobs: List[Job]


RegistryType = Dict[str, ModuleJobs]

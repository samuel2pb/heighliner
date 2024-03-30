from typing import Dict, Any


JOB_FLOW_OVERRIDES: Dict[str, Any] = {
    "Name": "PiCalc",
    "ReleaseLabel": "emr-6.7.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    # "Steps": SPARK_STEPS,
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

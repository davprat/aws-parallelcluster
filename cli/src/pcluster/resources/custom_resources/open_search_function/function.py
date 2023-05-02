import base64
import json
from datetime import datetime, timezone
from typing import Dict, Tuple, Union

import boto3

EC2_INSTANCE_HEALTHY_STATES = {"pending", "running"}
EC2_INSTANCE_STOP_STATES = {"stopping", "stopped"}
EC2_INSTANCE_ALIVE_STATES = EC2_INSTANCE_HEALTHY_STATES | EC2_INSTANCE_STOP_STATES
BOTO3_PAGINATION_PAGE_SIZE = 500


def _get_instance_mappings():
    ec2_client = boto3.client("ec2")
    paginator = ec2_client.get_paginator("describe_instances")
    args = {
        "Filters": [
            {"Name": "tag-key", "Values": ["parallelcluster:cluster-name"]},
            {"Name": "instance-state-name", "Values": list(EC2_INSTANCE_ALIVE_STATES)},
        ],
    }
    response_iterator = paginator.paginate(PaginationConfig={"PageSize": BOTO3_PAGINATION_PAGE_SIZE}, **args)
    filtered_iterator = response_iterator.search("Reservations[].Instances[]")
    return {
        instance_info["InstanceId"]: {"tags": {tag["Key"]: tag["Value"] for tag in instance_info["Tags"]}}
        for instance_info in filtered_iterator
    }


def _get_volume_mappings():
    ec2_client = boto3.client("ec2")
    paginator = ec2_client.get_paginator("describe_volumes")
    args = {
        "Filters": [
            {"Name": "tag-key", "Values": ["parallelcluster:cluster-name"]},
            {"Name": "status", "Values": ["in-use"]},
        ],
    }
    response_iterator = paginator.paginate(PaginationConfig={"PageSize": BOTO3_PAGINATION_PAGE_SIZE}, **args)
    filtered_iterator = response_iterator.search("Volumes[]")
    return {
        volume_info["VolumeId"]: {"tags": {tag["Key"]: tag["Value"] for tag in volume_info["Tags"]}}
        for volume_info in filtered_iterator
    }


def _update_metric(
    metric: str, instance_mappings: Dict[str, Dict], volume_mappings: Dict[str, Dict]
) -> Tuple[str, Union[str, None]]:
    if not metric:
        return "", None
    data = json.loads(metric)
    epoch_time = data.get("timestamp", 0)
    date_time = datetime.fromtimestamp(epoch_time / 1000.0, tz=timezone.utc)
    data.update({"datetime": date_time.isoformat(timespec="milliseconds")})
    instance_id = data.get("dimensions", {}).get("InstanceId", None)
    if instance_id:
        instance = instance_mappings.get(instance_id, None)
        if not instance:
            return "", None
        data.update({"tags": instance.get("tags", {})})
    volume_id = data.get("dimensions", {}).get("VolumeId", None)
    if volume_id:
        volume = volume_mappings.get(volume_id, None)
        if not volume:
            return "", None
        data.update({"tags": volume.get("tags", {})})

    return date_time.date().isoformat(), json.dumps(data)


def _transform_event_records(
    instance_mappings: Dict[str, Dict], volume_mappings: Dict[str, Dict], event: Dict[str, Dict]
):
    for record in event["records"]:
        record_id = record["recordId"]
        document_id = record_id.removesuffix("000")
        payload = base64.b64decode(record["data"]).decode("utf-8")
        sub_sequence = 1
        metrics = []
        for metric in payload.split("\n"):
            day, metric = _update_metric(metric, instance_mappings, volume_mappings)
            if metric:
                metrics.append(
                    f'{{"index":{{"_index":"parallelcluster-metrics-{day}", "_id": '
                    f'"{document_id}.{sub_sequence}"}}}}'
                )
                metrics.append(metric)
                sub_sequence += 1
        if metrics:
            payload = "\n".join(["{}", *metrics])
            print(payload.replace("\n", "\r"))
            yield {
                "recordId": record_id,
                "result": "Ok",
                "data": base64.b64encode(payload.encode("utf-8")).decode("utf-8"),
            }
        else:
            yield {
                "recordId": record_id,
                "result": "Dropped",
            }


def lambda_handler(event, context):
    instance_mappings = _get_instance_mappings()
    volume_mappings = _get_volume_mappings()
    print(f"Loaded {len(instance_mappings)} instances and {len(volume_mappings)} volumes")
    output = [record for record in _transform_event_records(instance_mappings, volume_mappings, event)]

    print("Successfully processed {} records.".format(len(output)))

    return {"records": output}

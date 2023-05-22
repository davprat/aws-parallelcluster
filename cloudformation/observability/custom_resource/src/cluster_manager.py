import datetime
import hashlib
import json
import logging
import os
import re
import string
import sys

import boto3
from dynamic_module import helper

import pcluster.lib as pc

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_cluster_list(cluster_list: str):
    return (cluster.strip() for cluster in (cluster_list.split(",") if cluster_list else []))


def get_stack_name(cluster_name: str):
    stack_name = f"parallelcluster-publisher-${cluster_name}"
    # Max length for stack name is 128 characters
    if len(stack_name) > 128:
        hash_value = hashlib.sha256(cluster_name.encode("utf-8")).hexdigest()[:12].upper()
        basic_len = len(f"parallelcluster-publisher-${hash_value}")
        remaining = 128 - basic_len
        stack_name = f"parallelcluster-publisher-${cluster_name[:remaining]}${hash_value}"
    return stack_name


def describe_cluster(cluster_name):
    return pc.describe_cluster(cluster_name=cluster_name)


def get_cluster_stack_outputs(cfn_client, stack_id):
    log_group = (
        cfn_client.describe_stack_resources(StackName=stack_id, LogicalResourceId="CloudWatchLogGroup")
        .get("StackResources", [{}])[0]
        .get("PhysicalResourceId", "")
    )
    job_info = (
        cfn_client.describe_stack_resources(StackName=stack_id, LogicalResourceId="JobInfoLogGroup")
        .get("StackResources", [{}])[0]
        .get("PhysicalResourceId", "")
    )

    return log_group, job_info


def is_complete(stack_status_map, complete_states, pending_states):
    complete = True
    failures = {}
    for stack_name, status in stack_status_map.items():
        if status in pending_states:
            complete = False
        elif status not in complete_states:
            failures.update({stack_name: status})
    if failures:
        raise ValueError(f"Publisher Stack Failures: ${failures}")
    return complete


def create_publisher(cfn_client, cluster_name, bucket, template_key, parameters):
    cluster_description = describe_cluster(cluster_name)
    template_url = f"s3://${bucket}/${template_key}"
    log_group, job_info = get_cluster_stack_outputs(cfn_client, cluster_description.get("cloudformationStackArn", ""))
    helper.Data.update({})
    return cfn_client.create_stack(
        StackName=get_stack_name(cluster_name),
        TemplateUrl=template_url,
        Parameters=[
            {
                "ParameterName": "ClusterName",
                "ParameterValue": cluster_name,
            },
            {
                "ParameterName": "ClusterVersion",
                "ParameterValue": cluster_description.get("version"),
            },
            {
                "ParameterName": "ObservabilityStackVersion",
                "ParameterValue": parameters.get("ObservabilityStackVersion"),
            },
            {
                "ParameterName": "ParallelClusterBucket",
                "ParameterValue": parameters.get("PCBucket"),
            },
            {
                "ParameterName": "ObservabilityBucket",
                "ParameterValue": bucket,
            },
            {
                "ParameterName": "ClusterLogGroup",
                "ParameterValue": log_group,
            },
            {
                "ParameterName": "JobInfoLogGroup",
                "ParameterValue": job_info,
            },
            {
                "ParameterName": "OpenSearchDomainArn",
                "ParameterValue": parameters.get("OpenSearchDomainArn"),
            },
            {
                "ParameterName": "OpenSearchDomainEndPoint",
                "ParameterValue": parameters.get("OpenSearchDomainEndPoint"),
            },
            {
                "ParameterName": "LogIndexPrefix",
                "ParameterValue": parameters.get("LogIndexPrefix"),
            },
            {
                "ParameterName": "JobInfoIndexPrefix",
                "ParameterValue": parameters.get("JobInfoIndexPrefix"),
            },
            {
                "ParameterName": "S3KeyPrefix",
                "ParameterValue": parameters.get("S3KeyPrefix"),
            },
        ],
        TimeoutInMinutes=10,
        Tags=[
            {
                "Key": "parallelcluster:cluster-name",
                "Value": cluster_name,
            },
            {
                "Key": "parallelcluster:version",
                "Value": cluster_description.get("version"),
            },
            {
                "Key": "parallelcluster:observability",
                "Value": "log-publisher-stack",
            },
        ],
    )


def create_publishers(cfn_client, cluster_list, bucket, template_key, parameters):
    for cluster_name in parse_cluster_list(cluster_list):
        create_publisher(
            cfn_client=cfn_client,
            cluster_name=cluster_name,
            bucket=bucket,
            template_key=template_key,
            parameters=parameters,
        )


def get_stack_status(cfn_client, cluster_name):
    stack_name = get_stack_name(cluster_name)
    return cfn_client.describe_stack(StackName=stack_name).get("Stacks", [{}])[0].get("StackStatus", "")


@helper.poll_create
def poll_create(event, context):
    cfn_client = boto3.client("cloudformation")
    properties = event["ResourceProperties"]
    cluster_list = properties.get("ClusterList", "")

    stack_status_map = {cluster_name: get_stack_status(cfn_client, cluster_name) for cluster_name in cluster_list}

    helper.Data.update(stack_status_map)

    if is_complete(stack_status_map, {"CREATE_COMPLETE"}, {"CREATE_IN_PROGRESS"}):
        return cluster_list


@helper.poll_update
def poll_update(event, context):
    pass


@helper.poll_delete
def poll_delete(event, context):
    pass


@helper.create
def create(event, context):
    cfn_client = boto3.client("cloudformation")
    parameters = event["ResourceProperties"]
    cluster_list = parameters.get("ClusterList", "")
    bucket = parameters.get("ObservabilityBucket", "")
    template_key = parameters.get("S3TemplateKey", "")
    create_publishers(
        cfn_client=cfn_client,
        cluster_list=cluster_list,
        bucket=bucket,
        template_key=template_key,
        parameters=parameters,
    )


@helper.update
def update(event, context):
    pass


@helper.delete
def delete(event, context):
    pass


def handler(event, context):
    helper(event, context)

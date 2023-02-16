import logging

import boto3
import pytest
from utils import get_vpc_snakecase_value


@pytest.fixture()
def subnet_factory(region, vpc_stack):
    az_subnet_map = {}

    ec2_client = boto3.client("ec2", region_name=region)

    vpc_id = vpc_stack.cfn_outputs["VpcId"]
    stack_name = vpc_stack.name

    cfn_client = boto3.client("cloudformation", region_name=region)
    nat_gateway_id = cfn_client.describe_stack_resources(StackName=stack_name, LogicalResourceId="NatGatewayPublic")[
        "StackResources"
    ][0]["PhysicalResourceId"]
    logging.info("Found NatGateway: %s", nat_gateway_id)

    def create_subnet(availability_zone):
        if availability_zone in az_subnet_map:
            return az_subnet_map.get(availability_zone).get("SubnetId")

        subnet = ec2_client.create_subnet(
            AvailabilityZone=availability_zone, CidrBlock=f"192.168.{240 + len(az_subnet_map)}.0/24", VpcId=vpc_id
        )

        logging.info("Created Subnet: ")

        subnet_id = subnet["Subnet"]["SubnetId"]
        logging.info("Created Subnet: %s", subnet_id)

        route_table = ec2_client.create_route_table(VpcId=vpc_id)
        route_table_id = route_table["RouteTable"]["RouteTableId"]

        logging.info("Created Route Table: %s", route_table_id)

        ec2_client.create_route(
            DestinationCidrBlock="0.0.0.0/0",
            NatGatewayId=nat_gateway_id,
            RouteTableId=route_table_id,
        )

        association = ec2_client.associate_route_table(RouteTableId=route_table_id, SubnetId=subnet_id)
        association_id = association["AssociationId"]

        logging.info("Created Association: %s", association_id)

        az_subnet_map.update(
            {
                availability_zone: {
                    "SubnetId": subnet_id,
                    "RouteTableId": route_table_id,
                    "AssociationId": association_id,
                }
            }
        )

        return subnet_id

    yield create_subnet

    for description in az_subnet_map.values():
        logging.info("Deleting: %s", description)
        ec2_client.disassociate_route_table(AssociationId=description["AssociationId"])
        ec2_client.delete_route(DestinationCidrBlock="0.0.0.0/0", RouteTableId=description["RouteTableId"])
        ec2_client.delete_route_table(RouteTableId=description["RouteTableId"])
        ec2_client.delete_subnet(SubnetId=description["SubnetId"])


@pytest.mark.usefixtures("region", "os", "instance", "scheduler")
@pytest.mark.test_cluster_creation
def test_cluster_creation(
    pcluster_config_reader,
    clusters_factory,
    test_datadir,
    s3_bucket_factory,
):
    # Create S3 bucket for pre-install scripts
    bucket_name = s3_bucket_factory()

    # Create S3 bucket for pre-install scripts
    cluster_config = pcluster_config_reader(
        bucket=bucket_name,
    )
    clusters_factory(cluster_config)


@pytest.mark.usefixtures("region", "os", "instance", "scheduler")
@pytest.mark.test_ice_failure
def test_ice_failure(
    pcluster_config_reader,
    clusters_factory,
    test_datadir,
    s3_bucket_factory,
    scheduler_commands_factory,
    subnet_factory,
):
    # Create S3 bucket for pre-install scripts
    bucket_name = s3_bucket_factory()

    subnet = subnet_factory(availability_zone="eu-west-2a")

    # Create S3 bucket for pre-install scripts
    cluster_config = pcluster_config_reader(
        compute_instance="c5d.24xlarge",
        compute_subnet_id=subnet,
        bucket=bucket_name,
    )
    cluster = clusters_factory(cluster_config)

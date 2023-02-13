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
@pytest.mark.test_slurm_protected_mode_on_cluster_create
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
@pytest.mark.test_slurm_protected_mode_on_cluster_create
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

    """

{'Subnet': {'AvailabilityZone': 'us-east-1e',
        'AvailabilityZoneId': 'use1-az3',
        'AvailableIpAddressCount': 507,
        'CidrBlock': '192.168.2.0/23',
        'DefaultForAz': False,
        'MapPublicIpOnLaunch': False,
        'State': 'available',
        'SubnetId': 'subnet-0ffb1eb350e959798',
        'VpcId': 'vpc-03cde5ccfe8e2425e',
        'OwnerId': '439493970194',
        'AssignIpv6AddressOnCreation': False,
        'Ipv6CidrBlockAssociationSet': [],
        'SubnetArn': 'arn:aws:ec2:us-east-1:439493970194:subnet/subnet-0ffb1eb350e959798',
        'EnableDns64': False,
        'Ipv6Native': False,
        'PrivateDnsNameOptionsOnLaunch': {'HostnameType': 'ip-name',
                                          'EnableResourceNameDnsARecord': False,
                                          'EnableResourceNameDnsAAAARecord': False
                                          }
      },
'ResponseMetadata': {'RequestId': '2b4258c8-c1e8-47d1-848c-3ff11f8f5645',
                  'HTTPStatusCode': 200,
                  'HTTPHeaders': {'x-amzn-requestid': '2b4258c8-c1e8-47d1-848c-3ff11f8f5645',
                                  'cache-control': 'no-cache, no-store',
                                  'strict-transport-security': 'max-age=31536000; '
                                                               'includeSubDomains',
                                  'content-type': 'text/xml;charset=UTF-8',
                                  'content-length': '1305',
                                  'date': 'Fri, 03 Feb 2023 19:44:06 GMT',
                                  'server': 'AmazonEC2'},
                  'RetryAttempts': 0}}

"NatRoutePrivate": {
"Properties": {
"DestinationCidrBlock": "0.0.0.0/0",
"NatGatewayId": {
 "Ref": "NatGatewayPublic"
},
"RouteTableId": {
 "Ref": "RouteTablePrivate"
}
},
"Type": "AWS::EC2::Route"
},

"RouteAssociationPrivate": {
"Properties": {
"RouteTableId": {
 "Ref": "RouteTablePrivate"
},
"SubnetId": {
 "Ref": "Private"
}
},
"Type": "AWS::EC2::SubnetRouteTableAssociation"
},

{'AssociationId': 'rtbassoc-0295fe21ccd52e6b2',
'AssociationState': {'State': 'associated'},
'ResponseMetadata': {'RequestId': '02448dd0-c5c8-4d05-b7ca-3d572018bdb7',
                  'HTTPStatusCode': 200,
                  'HTTPHeaders': {'x-amzn-requestid': '02448dd0-c5c8-4d05-b7ca-3d572018bdb7',
                                  'cache-control': 'no-cache, no-store',
                                  'strict-transport-security': 'max-age=31536000; '
                                                               'includeSubDomains',
                                  'content-type': 'text/xml;charset=UTF-8',
                                  'content-length': '356',
                                  'date': 'Fri, 03 Feb 2023 20:23:41 GMT',
                                  'server': 'AmazonEC2'},
                  'RetryAttempts': 0}}

{'RouteTable': {'Associations': [],
            'PropagatingVgws': [],
            'RouteTableId': 'rtb-0b6e2bf32f71911bf',
            'Routes': [{'DestinationCidrBlock': '192.168.0.0/17',
                        'GatewayId': 'local',
                        'Origin': 'CreateRouteTable',
                        'State': 'active'},
                       {'DestinationCidrBlock': '192.168.128.0/17',
                        'GatewayId': 'local',
                        'Origin': 'CreateRouteTable',
                        'State': 'active'}],
            'Tags': [],
            'VpcId': 'vpc-03cde5ccfe8e2425e',
            'OwnerId': '439493970194'},
'ResponseMetadata': {'RequestId': 'fd437833-fa5c-48e2-828f-201b51af0cd3',
                  'HTTPStatusCode': 200,
                  'HTTPHeaders': {'x-amzn-requestid': 'fd437833-fa5c-48e2-828f-201b51af0cd3',
                                  'cache-control': 'no-cache, no-store',
                                  'strict-transport-security': 'max-age=31536000; '
                                                               'includeSubDomains',
                                  'content-type': 'text/xml;charset=UTF-8',
                                  'content-length': '996',
                                  'date': 'Fri, 03 Feb 2023 20:09:39 GMT',
                                  'server': 'AmazonEC2'},
                  'RetryAttempts': 0}}

resource = cfn.describe_stack_resources(StackName="integ-tests-vpc-qeklf3qgcr0bxny6", LogicalResourceId="NatGatewayPublic")
{'StackResources': [{'StackName': 'integ-tests-vpc-qeklf3qgcr0bxny6',
                 'StackId': 'arn:aws:cloudformation:us-east-1:439493970194:stack/integ-tests-vpc-qeklf3qgcr0bxny6/2ad4ed30-80a5-11ed-9c5e-12dcfcb90ced',
                 'LogicalResourceId': 'NatGatewayPublic',
                 'PhysicalResourceId': 'nat-0d03ddab42332ecbe',
                 'ResourceType': 'AWS::EC2::NatGateway',
                 'Timestamp': datetime.datetime(2022, 12, 20, 20, 33, 16, 984000, tzinfo=tzutc()),
                 'ResourceStatus': 'CREATE_COMPLETE',
                 'DriftInformation': {'StackResourceDriftStatus': 'NOT_CHECKED'}}],
'ResponseMetadata': {'RequestId': '715661b0-eed6-4d4f-88ba-a8a08ba791d0',
                  'HTTPStatusCode': 200,
                  'HTTPHeaders': {'x-amzn-requestid': '715661b0-eed6-4d4f-88ba-a8a08ba791d0',
                                  'date': 'Fri, 03 Feb 2023 19:58:49 GMT',
                                  'content-type': 'text/xml',
                                  'content-length': '1024'},
                  'RetryAttempts': 0}}

    """

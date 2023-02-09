import boto3
import pytest
from assertpy import assert_that


@pytest.mark.usefixtures("os", "instance", "scheduler")
@pytest.mark.test_slurm_protected_mode_on_cluster_create
def test_cluster_create_failure(
    region,
    pcluster_config_reader,
    clusters_factory,
    test_datadir,
    s3_bucket_factory,
    scheduler_commands_factory,
):
    """Test that slurm protected mode triggers head node launch failure on cluster creation."""
    # assert_that(False).is_true()
    # raise SetupError("Hello")

    # Create S3 bucket for pre-install scripts
    bucket_name = s3_bucket_factory()
    bucket = boto3.resource("s3", region_name=region).Bucket(bucket_name)
    bucket.upload_file(str(test_datadir / "preinstall.sh"), "scripts/preinstall.sh")

    cluster_config = pcluster_config_reader(bucket=bucket_name)
    clusters_factory(cluster_config)


@pytest.mark.usefixtures("region", "os", "instance", "scheduler")
@pytest.mark.test_cluster_failure
def test_cluster_failure(
    pcluster_config_reader,
    clusters_factory,
    test_datadir,
    s3_bucket_factory,
    scheduler_commands_factory,
):
    """Test that slurm protected mode triggers head node launch failure on cluster creation."""
    assert_that(False).is_true()

import inspect
import logging
from types import FrameType
from typing import Callable, cast

import boto3
import pytest
from assertpy import assert_that


def frame_walker(frame_consumer: Callable[[FrameType], bool]):
    current_frame = cast(FrameType, inspect.currentframe().f_back)
    while current_frame:
        if not frame_consumer(current_frame):
            break
        current_frame = cast(FrameType, current_frame.f_back)


def log_frame_functions(context):
    def frame_consumer(frame: FrameType) -> bool:
        name = frame.f_code.co_name
        item = None
        if name in frame.f_globals:
            item = frame.f_globals[name]
        if name in frame.f_locals:
            item = frame.f_locals[name]
        if item and callable(item):
            logging.info("\t%s -> %s", context, name)
        return True

    frame_walker(frame_consumer)


def get_a_thing(context):
    log_frame_functions(context)


@pytest.fixture(scope="module")
def provide_a_thing():
    get_a_thing("Fixture")

    def get_a_thing_provider(context=None):
        if not context:
            context = "Under test"
        get_a_thing(context)
        return True

    return get_a_thing_provider


@pytest.fixture
def use_a_thing(provide_a_thing):
    provide_a_thing("Another Fixture")
    return provide_a_thing


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
    use_a_thing,
):
    """Test that slurm protected mode triggers head node launch failure on cluster creation."""
    use_a_thing()

    assert_that(False).is_true()

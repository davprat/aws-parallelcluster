import sys

crhelper_path = "/opt/python/pcluster/resources/custom_resources/custom_resources_code"
sys.path.insert(0, crhelper_path)
from crhelper import CfnResource

helper = CfnResource()

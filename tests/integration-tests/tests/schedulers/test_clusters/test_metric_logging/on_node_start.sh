#!/bin/bash
set -e
. "/etc/parallelcluster/cfnconfig"

echo "**** Installing custom_node_package: $1"
custom_node_package=$1
source /opt/parallelcluster/pyenv/versions/cookbook_virtualenv/bin/activate
aws s3 cp $custom_node_package /tmp/node.tgz
/opt/parallelcluster/pyenv/versions/node_virtualenv/bin/pip install --upgrade /tmp/node.tgz

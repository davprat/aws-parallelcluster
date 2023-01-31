#!/bin/bash
set -e
. "/etc/parallelcluster/cfnconfig"

# Patch file must be run from the root path
pushd /
sudo cat /tmp/pcluster.patch | patch -p0 -b

# Restart clustermgtd
source /opt/parallelcluster/pyenv/versions/cookbook_virtualenv/bin/activate
supervisorctl reload

popd

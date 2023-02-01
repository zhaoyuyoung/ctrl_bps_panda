#!/bin/bash
# setup Rubin env
export LSST_VERSION=w_2022_32
source /cvmfs/sw.lsst.eu/linux-x86_64/lsst_distrib/${LSST_VERSION}/loadLSST.bash
setup lsst_distrib

# setup PanDA env. Will be a simple step when the deployment of PanDA is fully done.
export PANDA_CONFIG_ROOT=$HOME
export PANDA_URL_SSL=https://pandaserver-doma.cern.ch:25443/server/panda
export PANDA_URL=http://pandaserver-doma.cern.ch:25080/server/panda
export PANDAMON_URL=https://panda-doma.cern.ch
export PANDA_AUTH=oidc
export PANDA_VERIFY_HOST=off
export PANDA_AUTH_VO=Rubin

# IDDS_CONFIG path depends on the weekly version
export PANDA_SYS=$CONDA_PREFIX
export IDDS_CONFIG=${PANDA_SYS}/etc/idds/idds.cfg.client.template

# WMS plugin
export BPS_WMS_SERVICE_CLASS=lsst.ctrl.bps.panda.PanDAService

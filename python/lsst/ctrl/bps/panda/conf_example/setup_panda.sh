#!/bin/bash
# To setup PanDA: source setup_panda.sh w_2022_32
# If using SDF: source setup_panda.sh w_2022_32 sdf

latest=$(ls -td /cvmfs/sw.lsst.eu/linux-x86_64/panda_env/v* | head -1)

usdf_cluster=$2
if [ "$usdf_cluster" == "sdf" ]; then
   setupScript=${latest}/setup_panda.sh
   echo "Working on cluster: " $usdf_cluster
else
   setupScript=${latest}/setup_panda_s3df.sh
fi
echo "setup from:" $setupScript

source $setupScript $1


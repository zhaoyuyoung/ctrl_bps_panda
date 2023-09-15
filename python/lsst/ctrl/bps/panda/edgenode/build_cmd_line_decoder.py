#!/usr/bin/python
"""
This file is needed to decode the command line string sent from the BPS
plugin -> PanDA -> Edge node cluster management
-> Edge node -> Container. This file is not a part
of the BPS but a part of the payload wrapper.
It decodes the hexified command line.
"""
# import base64
import datetime
import logging
import os
import sys
import tarfile

from lsst.ctrl.bps.bps_utils import _create_execution_butler
from lsst.ctrl.bps.constants import DEFAULT_MEM_FMT, DEFAULT_MEM_UNIT
from lsst.ctrl.bps.drivers import prepare_driver
from lsst.ctrl.bps.panda.constants import PANDA_DEFAULT_MAX_COPY_WORKERS
from lsst.ctrl.bps.panda.utils import copy_files_for_distribution, get_idds_client
from lsst.utils.timer import time_this

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s\t%(threadName)s\t%(name)s\t%(levelname)s\t%(message)s",
)

_LOG = logging.getLogger(__name__)


def download_extract_archive(filename):
    archive_basename = os.path.basename(filename)
    target_dir = os.getcwd()
    full_output_filename = os.path.join(target_dir, archive_basename)

    if filename.startswith("https:"):
        panda_cache_url = os.path.dirname(os.path.dirname(filename))
        os.environ["PANDACACHE_URL"] = panda_cache_url
    elif "PANDACACHE_URL" not in os.environ and "PANDA_URL_SSL" in os.environ:
        os.environ["PANDACACHE_URL"] = os.environ["PANDA_URL_SSL"]
    print("PANDACACHE_URL: %s" % os.environ.get("PANDACACHE_URL", None))

    from pandaclient import Client

    status, output = Client.getFile(archive_basename, output_path=full_output_filename)
    print("Download archive file from pandacache status: %s, output: %s" % (status, output))
    if status != 0:
        raise RuntimeError("Failed to download archive file from pandacache")
    with tarfile.open(full_output_filename, "r:gz") as f:
        f.extractall(target_dir)
    print("Extract %s to %s" % (full_output_filename, target_dir))
    os.remove(full_output_filename)
    print("Remove %s" % full_output_filename)


def create_idds_workflow(config_file):
    _LOG.info("Starting building process")
    with time_this(
        log=_LOG,
        level=logging.INFO,
        prefix=None,
        msg="Completed entire submission process",
        mem_usage=True,
        mem_unit=DEFAULT_MEM_UNIT,
        mem_fmt=DEFAULT_MEM_FMT,
    ):
        wms_workflow_config, wms_workflow = prepare_driver(config_file)
        _, when_create = wms_workflow_config.search(".executionButler.whenCreate")
        if when_create.upper() == "SUBMIT":
            _, execution_butler_dir = wms_workflow_config.search(".bps_defined.executionButlerDir")
            _LOG.info("Creating execution butler in '%s'", execution_butler_dir)
            with time_this(
                log=_LOG, level=logging.INFO, prefix=None, msg="Completed creating execution butler"
            ):
                _create_execution_butler(
                    wms_workflow_config,
                    wms_workflow_config["runQgraphFile"],
                    execution_butler_dir,
                    wms_workflow_config["submitPath"],
                )
    return wms_workflow_config, wms_workflow


# download the submission tarball
remote_filename = sys.argv[1]
download_extract_archive(remote_filename)

# request_id and signature are added by iDDS for build task
request_id = os.environ.get("IDDS_BUILD_REQUEST_ID", None)
signature = os.environ.get("IDDS_BUIL_SIGNATURE", None)
config_file = sys.argv[2]

if request_id is None:
    print("IDDS_BUILD_REQUEST_ID is not defined.")
    sys.exit(-1)
if signature is None:
    print("IDDS_BUIL_SIGNATURE is not defined")
    sys.exit(-1)

print("INFO: start {}".format(datetime.datetime.utcnow()))
print("INFO: config file: {}".format(config_file))

current_dir = os.getcwd()

print("INFO: current dir: %s" % current_dir)

config, bps_workflow = create_idds_workflow(config_file)
idds_workflow = bps_workflow.idds_client_workflow

_, max_copy_workers = config.search("maxCopyWorkers", opt={"default": PANDA_DEFAULT_MAX_COPY_WORKERS})
copy_files_for_distribution(
    bps_workflow.files_to_pre_stage, config["fileDistributionEndPoint"], max_copy_workers
)

idds_client = get_idds_client(config)
ret = idds_client.update_build_request(request_id, signature, idds_workflow)
print("update_build_request returns: %s" % str(ret))
sys.exit(ret[0])

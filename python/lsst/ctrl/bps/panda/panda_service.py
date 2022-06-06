# This file is part of ctrl_bps_panda.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


__all__ = ["PanDAService", "PandaBpsWmsWorkflow"]


import binascii
import concurrent.futures
import json
import logging
import os

import idds.common.utils as idds_utils
import pandaclient.idds_api
from idds.doma.workflowv2.domapandawork import DomaPanDAWork
from idds.workflowv2.workflow import AndCondition
from idds.workflowv2.workflow import Workflow as IDDS_client_workflow
from lsst.ctrl.bps.bps_config import BpsConfig
from lsst.ctrl.bps.panda.idds_tasks import IDDSWorkflowGenerator

# from lsst.ctrl.bps.panda.panda_auth_utils import panda_auth_update
from lsst.ctrl.bps.wms_service import BaseWmsService, BaseWmsWorkflow, WmsRunReport, WmsStates
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


class PanDAService(BaseWmsService):
    """PanDA version of WMS service"""

    def prepare(self, config, generic_workflow, out_prefix=None):
        """Convert generic workflow to an PanDA iDDS ready for submission

        Parameters
        ----------
        config : `lsst.ctrl.bps.BpsConfig`
            BPS configuration that includes necessary submit/runtime
            information.
        generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        out_prefix : `str`
            The root directory into which all WMS-specific files are written

        Returns
        -------
        workflow : `lsst.ctrl.bps.panda.panda_service.PandaBpsWmsWorkflow`
            PanDA workflow ready to be run.
        """
        _LOG.debug("out_prefix = '%s'", out_prefix)
        workflow = PandaBpsWmsWorkflow.from_generic_workflow(
            config, generic_workflow, out_prefix, f"{self.__class__.__module__}." f"{self.__class__.__name__}"
        )
        workflow.write(out_prefix)
        return workflow

    def convert_exec_string_to_hex(self, cmdline):
        """Convert the command line into hex representation.

        This step is currently involved because large blocks of command lines
        including special symbols passed to the pilot/container. To make sure
        the 1 to 1 matching and pass by the special symbol stripping
        performed by the Pilot we applied the hexing.

        Parameters
        ----------
        cmdline : `str`
            UTF-8 command line string

        Returns
        -------
        hex : `str`
            Hex representation of string
        """
        return binascii.hexlify(cmdline.encode()).decode("utf-8")

    def add_decoder_prefix(self, cmd_line, distribution_path, files):
        """
        Compose the command line sent to the pilot from the functional part
        (the actual SW running) and the middleware part (containers invocation)

        Parameters
        ----------
        cmd_line : `str`
            UTF-8 based functional part of the command line
        distribution_path : `str`
            URI of path where all files are located for distribution
        files `list` [`str`]
            File names needed for a task

        Returns
        -------
        decoder_prefix : `str`
            Full command line to be executed on the edge node
        """

        cmdline_hex = self.convert_exec_string_to_hex(cmd_line)
        _, decoder_prefix = self.config.search(
            "runnerCommand", opt={"replaceEnvVars": False, "expandEnvVars": False}
        )
        decoder_prefix = decoder_prefix.replace(
            "_cmd_line_",
            str(cmdline_hex)
            + " ${IN/L} "
            + distribution_path
            + "  "
            + "+".join(f"{k}:{v}" for k, v in files[0].items())
            + " "
            + "+".join(files[1]),
        )
        return decoder_prefix

    def submit(self, workflow):
        """Submit a single PanDA iDDS workflow

        Parameters
        ----------
        workflow : `lsst.ctrl.bps.BaseWorkflow`
            A single PanDA iDDS workflow to submit
        """
        idds_client_workflow = IDDS_client_workflow(name=workflow.name)
        files = self.copy_files_for_distribution(
            workflow.generated_tasks, self.config["fileDistributionEndPoint"]
        )
        DAG_end_work = []
        DAG_final_work = None

        _, processing_type = self.config.search("processing_type", opt={"default": None})
        _, task_type = self.config.search("task_type", opt={"default": "test"})
        _, prod_source_label = self.config.search("prod_source_label", opt={"default": "test"})
        _, vo = self.config.search("vo", opt={"default": "wlcg"})

        for idx, task in enumerate(workflow.generated_tasks):
            work = DomaPanDAWork(
                executable=self.add_decoder_prefix(
                    task.executable, self.config["fileDistributionEndPoint"], files
                ),
                primary_input_collection={
                    "scope": "pseudo_dataset",
                    "name": "pseudo_input_collection#" + str(idx),
                },
                output_collections=[
                    {"scope": "pseudo_dataset", "name": "pseudo_output_collection#" + str(idx)}
                ],
                log_collections=[],
                dependency_map=task.dependencies,
                task_name=task.name,
                task_queue=task.queue,
                task_log={
                    "destination": "local",
                    "value": "log.tgz",
                    "dataset": "PandaJob_#{pandaid}/",
                    "token": "local",
                    "param_type": "log",
                    "type": "template",
                },
                encode_command_line=True,
                task_rss=task.max_rss,
                task_cloud=task.cloud,
                task_site=task.site,
                task_priority=int(task.priority) if task.priority else 900,
                core_count=task.core_count,
                working_group=task.working_group,
                processing_type=processing_type,
                task_type=task_type,
                prodSourceLabel=task.prod_source_label if task.prod_source_label else prod_source_label,
                vo=vo,
                maxattempt=task.max_attempt,
                maxwalltime=task.max_walltime if task.max_walltime else 90000,
            )

            idds_client_workflow.add_work(work)
            if task.is_final:
                DAG_final_work = work
            if task.is_dag_end:
                DAG_end_work.append(work)

        if DAG_final_work:
            conditions = []
            for work in DAG_end_work:
                conditions.append(work.is_terminated)
            and_cond = AndCondition(conditions=conditions, true_works=[DAG_final_work])
            idds_client_workflow.add_condition(and_cond)
        c = self.get_idds_client()
        ret = c.submit(idds_client_workflow, username=None, use_dataset_name=False)
        _LOG.debug("iDDS client manager submit returned = %s", str(ret))

        # Check submission success
        # https://panda-wms.readthedocs.io/en/latest/client/rest_idds.html
        if ret[0] == 0 and ret[1][0]:
            request_id = int(ret[1][-1])
        else:
            raise RuntimeError(f"Error submitting to PanDA service: {str(ret)}")

        _LOG.info("Submitted into iDDs with request id=%s", request_id)
        workflow.run_id = request_id

    @staticmethod
    def copy_files_for_distribution(tasks, file_distribution_uri):
        """
        Brings locally generated files into Cloud for further
        utilization them on the edge nodes.

        Parameters
        ----------
        local_pfns: `list` of `tasks`
            Tasks that input files needs to be placed for
            distribution
        file_distribution_uri: `str`
            Path on the edge node accessed storage,
            including access protocol, bucket name to place files

        Returns
        -------
        files_plc_hldr, direct_IO_files : `dict` [`str`, `str`], `set` of `str`
            First parameters is key values pairs
            of file placeholder - file name
            Second parameter is set of files which will be directly accessed.
        """
        local_pfns = {}
        direct_IO_files = set()
        for task in tasks:
            for file in task.files_used_by_task:
                if not file.delivered:
                    local_pfns[file.name] = file.submission_url
                    if file.direct_IO:
                        direct_IO_files.add(file.name)

        files_to_copy = {}

        # In case there are folders we iterate over its content
        for local_pfn in local_pfns.values():
            folder_name = os.path.basename(local_pfn)
            if os.path.isdir(local_pfn):
                files_in_folder = ResourcePath.findFileResources([local_pfn])
                for file in files_in_folder:
                    file_name = file.basename()
                    files_to_copy[file] = ResourcePath(
                        os.path.join(file_distribution_uri, folder_name, file_name)
                    )
            else:
                files_to_copy[ResourcePath(local_pfn)] = ResourcePath(
                    os.path.join(file_distribution_uri, folder_name)
                )

        copy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
        future_file_copy = []
        for src, trgt in files_to_copy.items():

            # S3 clients explicitly instantiate here to overpass this
            # https://stackoverflow.com/questions/52820971/is-boto3-client-thread-safe
            trgt.exists()
            future_file_copy.append(copy_executor.submit(trgt.transfer_from, src, transfer="copy"))
        for future in concurrent.futures.as_completed(future_file_copy):
            if not future.result() is None:
                raise RuntimeError("Error of placing files to the distribution point")

        if len(direct_IO_files) == 0:
            direct_IO_files.add("cmdlineplaceholder")

        files_plc_hldr = {}
        for file_placeholder, src_path in local_pfns.items():
            files_plc_hldr[file_placeholder] = os.path.basename(src_path)
            if os.path.isdir(src_path):
                # this is needed to make isdir function working
                # properly in ButlerURL instance on the egde node
                files_plc_hldr[file_placeholder] += "/"

        return files_plc_hldr, direct_IO_files

    def get_idds_client(self):
        """ Get the idds client
        Returns
        -------
        idds_client: `iDDS client manager object`
        """
        idds_server = None
        if type(self.config) in [BpsConfig]:
            _, idds_server = self.config.search("iddsServer", opt={"default": None})
        elif type(self.config) in [dict] and 'iddsServer' in self.config:
            idds_server = self.config['iddsServer']
        idds_client = pandaclient.idds_api.get_api(
            idds_utils.json_dumps, idds_host=idds_server, compress=True, manager=True
        )
        return idds_client

    def restart(self, wms_workflow_id):
        """Restart a workflow from the point of failure.
        Parameters
        ----------
        wms_workflow_id : `str`
            Id that can be used by WMS service to identify workflow that
            need to be restarted.
        Returns
        -------
        wms_id : `str`
            Id of the restarted workflow. If restart failed, it will be set
            to None.
        run_name : `str`
            Name of the restarted workflow. If restart failed, it will be set
            to None.
        message : `str`
            A message describing any issues encountered during the restart.
            If there were no issue, an empty string is returned.
        """
        c = self.get_idds_client()
        ret = c.retry(request_id=wms_workflow_id)
        _LOG.debug("Retry PanDA workflow returned = %s", str(ret))

        rets = []
        success = True
        if ret[0] == 0 and ret[1][0]:

            for req in ret[1][1]:
                if type(req) in [tuple, list]:
                    if req[0] == 0:
                        _LOG.info(req[1])
                        rets.append(req[1])
                    else:
                        _LOG.error(req[1])
                        rets.append(req[1])
                else:
                    _LOG.info(req)
                    rets.append(req)
        if not success:
            raise RuntimeError(f"Error to retry workflow: {str(ret)}")
        return True, None, json.dumps(rets)

    def convert_idds_state_to_wms_state(self, state):
        """Convert iDDS state to BPS wms state
        Parameters
        ----------
        state : `str`:
            iDDS task state.

        Returns
        -------
        wms_state: `WmsStates`
            BPS wms state
        """
        if state in ['New']:
            wms_state = WmsStates.UNREADY
        elif state in ['Ready']:
            wms_state = WmsStates.READY
        elif state in ['Transforming']:
            wms_state = WmsStates.RUNNING
        elif state in ['Finished', 'SubFinished']:
            wms_state = WmsStates.SUCCEEDED
        elif state in ['Cancelled', 'Suspended']:
            wms_state = WmsStates.HELD
        elif state in ['Failed', 'Expired']:
            wms_state = WmsStates.FAILED
        else:
            wms_state = WmsStates.UNKNOWN
        return wms_state

    def report(self, wms_workflow_id=None, user=None, hist=0, pass_thru=None, is_global=False):
        """Stub for future implementation of the report method
        Expected to return run information based upon given constraints.

        Parameters
        ----------
        wms_workflow_id : `int` or `str`
            Limit to specific run based on id.
        user : `str`
            Limit results to runs for this user.
        hist : `float`
            Limit history search to this many days.
        pass_thru : `str`
            Constraints to pass through to HTCondor.
        is_global : `bool`, optional
            If set, all available job queues will be queried for job
            information. Defaults to False which means that only a local job
            queue will be queried for information.

        Returns
        -------
        runs : `list` [`lsst.ctrl.bps.WmsRunReport`]
            Information about runs from given job information.
        message : `str`
            Extra message for report command to print.  This could be
            pointers to documentation or to WMS specific commands.
        """
        message = ""
        run_reports = []

        c = self.get_idds_client()
        ret = c.get_requests(request_id=wms_workflow_id, with_detail=True)
        _LOG.debug("PanDA get workflow status returned = %s", str(ret))

        success = False
        if ret[0] == 0 and ret[1][0]:
            reqs = ret[1][1]
            for req in reqs:
                transform_status = req['transform_status']['attributes']['_name_']
                wms_state = self.convert_idds_state_to_wms_state(transform_status)
                job_state_counts = {}
                for state in WmsStates:
                    job_state_counts[state] = 0
                job_state_counts[WmsStates.SUCCEEDED] = req['output_processed_files']
                job_state_counts[WmsStates.RUNNING] = req['output_processing_files']
                report = {"wms_id": str(req['request_id']),
                          "global_wms_id": None,
                          "path": None,
                          "label": None,
                          "run": str(req['transform_workload_id']),
                          "project": "Rubin",
                          "campaign": "Rubin",
                          "payload": req['name'],
                          "operator": req['username'],
                          "run_summary": None,
                          "state": wms_state,
                          "total_number_jobs": req['output_total_files'],
                          "jobs": [],
                          "job_state_counts": job_state_counts,
                          }

                wms_report = WmsRunReport(**report)
                run_reports.append(wms_report)
                success = True

        if not success:
            raise RuntimeError(f"Error to get workflow status: {str(ret)}")

        return run_reports, message

    def list_submitted_jobs(self, wms_id=None, user=None, require_bps=True, pass_thru=None, is_global=False):
        """Query WMS for list of submitted WMS workflows/jobs.

        This should be a quick lookup function to create list of jobs for
        other functions.

        Parameters
        ----------
        wms_id : `int` or `str`, optional
            Id or path that can be used by WMS service to look up job.
        user : `str`, optional
            User whose submitted jobs should be listed.
        require_bps : `bool`, optional
            Whether to require jobs returned in list to be bps-submitted jobs.
        pass_thru : `str`, optional
            Information to pass through to WMS.
        is_global : `bool`, optional
            If set, all available job queues will be queried for job
            information.  Defaults to False which means that only a local job
            queue will be queried for information.

            Only applicable in the context of a WMS using distributed job
            queues (e.g., HTCondor). A WMS with a centralized job queue
            (e.g. PanDA) can safely ignore it.

        Returns
        -------
        req_ids : `list` [`Any`]
            Only job ids to be used by cancel and other functions.  Typically
            this means top-level jobs (i.e., not children jobs).
        """
        c = self.get_idds_client()
        ret = c.get_requests(request_id=wms_id)
        _LOG.debug("PanDA get workflows returned = %s", str(ret))

        req_ids = []
        success = False
        if ret[0] == 0 and ret[1][0]:
            reqs = ret[1][1]
            for req in reqs:
                req_ids.append(req['request_id'])
                success = True

        if not success:
            raise RuntimeError(f"Error to get workflows: {str(ret)}")

        return req_ids

    def cancel(self, wms_id, pass_thru=None):
        """Cancel submitted workflows/jobs.
        Parameters
        ----------
        wms_id : `str`
            ID or path of job that should be canceled.
        pass_thru : `str`, optional
            Information to pass through to WMS.
        Returns
        -------
        deleted : `bool`
            Whether successful deletion or not.  Currently, if any doubt or any
            individual jobs not deleted, return False.
        message : `str`
            Any message from WMS (e.g., error details).
        """
        c = self.get_idds_client()
        ret = c.abort(request_id=wms_id)
        _LOG.debug("Abort PanDA workflow returned = %s", str(ret))

        rets = []
        success = True
        if ret[0] == 0 and ret[1][0]:
            for req in ret[1][1]:
                if type(req) in [tuple, list]:
                    if req[0] == 0:
                        _LOG.info(req[1])
                        rets.append(req[1])
                    else:
                        _LOG.error(req[1])
                        rets.append(req[1])
                else:
                    _LOG.info(req)
                    rets.append(req)
        if not success:
            raise RuntimeError(f"Error to abort workflow: {str(ret)}")
        return True, json.dumps(rets)

    def ping(self):
        """Ping iDDS server.
        Returns
        -------
        success : `bool`
            Whether to successfully ping the iDDS server.
        """
        c = self.get_idds_client()
        ret = c.ping()
        _LOG.debug("Ping PanDA service returned = %s", str(ret))

        success = False
        # Check submission success
        # https://panda-wms.readthedocs.io/en/latest/client/rest_idds.html
        if ret[0] == 0 and ret[1][0]:
            ret_status = ret[1][1]
            if 'Status' in ret_status and ret_status['Status'] == 'OK':
                success = True
        if not success:
            raise RuntimeError(f"Error to ping PanDA service: {str(ret)}")

    def run_submission_checks(self):
        """Checks to run at start if running WMS specific submission steps.

        Any exception other than NotImplementedError will halt submission.
        Submit directory may not yet exist when this is called.
        """
        for key in ["PANDA_URL"]:
            if key not in os.environ:
                raise OSError(f"Missing environment variable {key}")

        self.ping()


class PandaBpsWmsWorkflow(BaseWmsWorkflow):
    """A single Panda based workflow

    Parameters
    ----------
    name : `str`
        Unique name for Workflow
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration that includes necessary submit/runtime information
    """

    def __init__(self, name, config=None):
        super().__init__(name, config)
        self.generated_tasks = None

    @classmethod
    def from_generic_workflow(cls, config, generic_workflow, out_prefix, service_class):
        # Docstring inherited from parent class
        idds_workflow = cls(generic_workflow.name, config)
        workflow_generator = IDDSWorkflowGenerator(generic_workflow, config)
        idds_workflow.generated_tasks = workflow_generator.define_tasks()
        _LOG.debug("panda dag attribs %s", generic_workflow.run_attrs)
        return idds_workflow

    def write(self, out_prefix):
        """Not yet implemented"""

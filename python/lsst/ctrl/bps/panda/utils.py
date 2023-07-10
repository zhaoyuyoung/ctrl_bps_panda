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

"""Utilities for bps PanDA plugin."""

__all__ = [
    "copy_files_for_distribution",
    "get_idds_client",
    "get_idds_result",
    "convert_exec_string_to_hex",
    "add_decoder_prefix",
]

import binascii
import concurrent.futures
import logging
import os

import idds.common.utils as idds_utils
import pandaclient.idds_api
from idds.doma.workflowv2.domapandawork import DomaPanDAWork
from idds.workflowv2.workflow import AndCondition
from lsst.ctrl.bps import BpsConfig, GenericWorkflow, GenericWorkflowJob
from lsst.ctrl.bps.panda.cmd_line_embedder import CommandLineEmbedder
from lsst.ctrl.bps.panda.constants import (
    PANDA_DEFAULT_CLOUD,
    PANDA_DEFAULT_CORE_COUNT,
    PANDA_DEFAULT_MAX_ATTEMPTS,
    PANDA_DEFAULT_MAX_JOBS_PER_TASK,
    PANDA_DEFAULT_MAX_WALLTIME,
    PANDA_DEFAULT_PRIORITY,
    PANDA_DEFAULT_PROCESSING_TYPE,
    PANDA_DEFAULT_PROD_SOURCE_LABEL,
    PANDA_DEFAULT_RSS,
    PANDA_DEFAULT_TASK_TYPE,
    PANDA_DEFAULT_VO,
)
from lsst.resources import ResourcePath

_LOG = logging.getLogger(__name__)


def copy_files_for_distribution(files_to_stage, file_distribution_uri, max_copy_workers):
    """Brings locally generated files into Cloud for further
    utilization them on the edge nodes.

    Parameters
    ----------
    local_pfns : `dict` [`str`, `str`]
        Files which need to be copied to a workflow staging area.
    file_distribution_uri: `str`
        Path on the edge node accessed storage,
        including access protocol, bucket name to place files.
    max_copy_workers : `int`
        Maximum number of workers for copying files.

    Raises
    ------
    RuntimeError
        Raised when error copying files to the distribution point.
    """
    files_to_copy = {}

    # In case there are folders we iterate over its content
    for local_pfn in files_to_stage.values():
        folder_name = os.path.basename(os.path.normpath(local_pfn))
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

    copy_executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_copy_workers)
    future_file_copy = []
    for src, trgt in files_to_copy.items():
        _LOG.debug("Staging %s to %s", src, trgt)
        # S3 clients explicitly instantiate here to overpass this
        # https://stackoverflow.com/questions/52820971/is-boto3-client-thread-safe
        trgt.exists()
        future_file_copy.append(copy_executor.submit(trgt.transfer_from, src, transfer="copy"))

    for future in concurrent.futures.as_completed(future_file_copy):
        if future.result() is not None:
            raise RuntimeError("Error of placing files to the distribution point")


def get_idds_client(config):
    """Get the idds client.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.

    Returns
    -------
    idds_client: `idds.client.clientmanager.ClientManager`
        iDDS ClientManager object.
    """
    idds_server = None
    if isinstance(config, BpsConfig):
        _, idds_server = config.search("iddsServer", opt={"default": None})
    elif isinstance(config, dict) and "iddsServer" in config:
        idds_server = config["iddsServer"]
    # if idds_server is None, a default value on the panda relay service
    # will be used
    idds_client = pandaclient.idds_api.get_api(
        idds_utils.json_dumps, idds_host=idds_server, compress=True, manager=True
    )
    return idds_client


def get_idds_result(ret):
    """Parse the results returned from iDDS.

    Parameters
    ----------
    ret: `tuple` of (`int`, (`bool`, payload)).
        The first part ret[0] is the status of PanDA relay service.
        The part of ret[1][0] is the status of iDDS service.
        The part of ret[1][1] is the returned payload.
        If ret[1][0] is False, ret[1][1] can be error messages.

    Returns
    -------
    status: `bool`
        The status of iDDS calls.
    result: `int` or `list` or `dict`
        The result returned from iDDS.
    error: `str`
        Error messages.
    """
    # https://panda-wms.readthedocs.io/en/latest/client/rest_idds.html
    if not isinstance(ret, list | tuple) or ret[0] != 0:
        # Something wrong with the PanDA relay service.
        # The call may not be delivered to iDDS.
        status = False
        result = None
        error = f"PanDA relay service returns errors: {str(ret)}"
    else:
        if ret[1][0]:
            status = True
            result = ret[1][1]
            error = None
            if isinstance(result, str) and "Authentication no permission" in result:
                status = False
                result = None
                error = result
        else:
            # iDDS returns errors
            status = False
            result = None
            error = f"iDDS returns errors: {str(ret[1][1])}"
    return status, result, error


def _make_pseudo_filename(config, gwjob):
    """Make the job pseudo filename.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job for which to create the pseudo filename.

    Returns
    -------
    pseudo_filename : `str`
        The pseudo filename for the given job.
    """
    cmd_line_embedder = CommandLineEmbedder(config)
    _, pseudo_filename = cmd_line_embedder.substitute_command_line(
        gwjob.executable.src_uri + " " + gwjob.arguments, gwjob.cmdvals, gwjob.name, []
    )
    return pseudo_filename


def _make_doma_work(config, generic_workflow, gwjob, task_count, task_chunk):
    """Make the DOMA Work object for a PanDA task.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    gwjob : `lsst.ctrl.bps.GenericWorkflowJob`
        Job representing the jobs for the PanDA task.
    task_count : `int`
        Count of PanDA tasks used when making unique names.
    task_chunk : `int`
        Count of chunk of a PanDA tasks used when making unique names.

    Returns
    -------
    work : `idds.doma.workflowv2.domapandawork.DomaPanDAWork`
        The client representation of a PanDA task.
    local_pfns : `dict` [`str`, `str`]
        Files which need to be copied to a workflow staging area.
    """
    _LOG.debug("Using gwjob %s to create new PanDA task (gwjob=%s)", gwjob.name, gwjob)
    cvals = {"curr_cluster": gwjob.label}
    _, site = config.search("computeSite", opt={"curvals": cvals, "required": True})
    cvals["curr_site"] = site
    _, processing_type = config.search(
        "processing_type", opt={"curvals": cvals, "default": PANDA_DEFAULT_PROCESSING_TYPE}
    )
    _, task_type = config.search("taskType", opt={"curvals": cvals, "default": PANDA_DEFAULT_TASK_TYPE})
    _, prod_source_label = config.search(
        "prodSourceLabel", opt={"curvals": cvals, "default": PANDA_DEFAULT_PROD_SOURCE_LABEL}
    )
    _, vo = config.search("vo", opt={"curvals": cvals, "default": PANDA_DEFAULT_VO})

    _, file_distribution_end_point = config.search(
        "fileDistributionEndPoint", opt={"curvals": cvals, "default": None}
    )

    _, file_distribution_end_point_default = config.search(
        "fileDistributionEndPointDefault", opt={"curvals": cvals, "default": None}
    )

    # Assume input files are same across task
    local_pfns = {}
    direct_io_files = set()

    if gwjob.executable.transfer_executable:
        local_pfns["job_executable"] = gwjob.executable.src_uri
        job_executable = f"./{os.path.basename(gwjob.executable.src_uri)}"
    else:
        job_executable = gwjob.executable.src_uri
    cmd_line_embedder = CommandLineEmbedder(config)
    _LOG.debug(
        "job %s inputs = %s, outputs = %s",
        gwjob.name,
        generic_workflow.get_job_inputs(gwjob.name),
        generic_workflow.get_job_outputs(gwjob.name),
    )

    cmd_line, _ = cmd_line_embedder.substitute_command_line(
        job_executable + " " + gwjob.arguments,
        gwjob.cmdvals,
        gwjob.name,
        generic_workflow.get_job_inputs(gwjob.name) + generic_workflow.get_job_outputs(gwjob.name),
    )

    for gwfile in generic_workflow.get_job_inputs(gwjob.name, transfer_only=True):
        local_pfns[gwfile.name] = gwfile.src_uri
        if os.path.isdir(gwfile.src_uri):
            # this is needed to make isdir function working
            # properly in ButlerURL instance on the edge node
            local_pfns[gwfile.name] += "/"

        if gwfile.job_access_remote:
            direct_io_files.add(gwfile.name)

    if not direct_io_files:
        direct_io_files.add("cmdlineplaceholder")

    lsst_temp = "LSST_RUN_TEMP_SPACE"
    if lsst_temp in file_distribution_end_point and lsst_temp not in os.environ:
        file_distribution_end_point = file_distribution_end_point_default

    executable = add_decoder_prefix(
        config, cmd_line, file_distribution_end_point, (local_pfns, direct_io_files)
    )
    work = DomaPanDAWork(
        executable=executable,
        primary_input_collection={
            "scope": "pseudo_dataset",
            "name": f"pseudo_input_collection#{str(task_count)}",
        },
        output_collections=[
            {"scope": "pseudo_dataset", "name": f"pseudo_output_collection#{str(task_count)}"}
        ],
        log_collections=[],
        dependency_map=[],
        task_name=f"{generic_workflow.name}_{task_count:02d}_{gwjob.label}_{task_chunk:02d}",
        task_queue=gwjob.queue,
        task_log={
            "destination": "local",
            "value": "log.tgz",
            "dataset": "PandaJob_#{pandaid}/",
            "token": "local",
            "param_type": "log",
            "type": "template",
        },
        encode_command_line=True,
        task_rss=gwjob.request_memory if gwjob.request_memory else PANDA_DEFAULT_RSS,
        task_cloud=gwjob.compute_cloud if gwjob.compute_cloud else PANDA_DEFAULT_CLOUD,
        task_site=site,
        task_priority=int(gwjob.priority) if gwjob.priority else PANDA_DEFAULT_PRIORITY,
        core_count=gwjob.request_cpus if gwjob.request_cpus else PANDA_DEFAULT_CORE_COUNT,
        working_group=gwjob.accounting_group,
        processing_type=processing_type,
        task_type=task_type,
        prodSourceLabel=prod_source_label,
        vo=vo,
        maxattempt=gwjob.number_of_retries if gwjob.number_of_retries else PANDA_DEFAULT_MAX_ATTEMPTS,
        maxwalltime=gwjob.request_walltime if gwjob.request_walltime else PANDA_DEFAULT_MAX_WALLTIME,
    )
    return work, local_pfns


def add_final_idds_work(
    config, generic_workflow, idds_client_workflow, dag_sink_work, task_count, task_chunk
):
    """Add the special final PanDA task to the client workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration.
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow in which to find the final job.
    idds_client_workflow : `idds.workflowv2.workflow.Workflow`
        iDDS client representation of the workflow to which the final task
        is added.
    dag_sink_work : `list` [`idds.doma.workflowv2.domapandawork.DomaPanDAWork`]
        The work nodes in the client workflow which have no successors.
    task_count : `int`
        Count of PanDA tasks used when making unique names.
    task_chunk : `int`
        Count of chunk of a PanDA tasks used when making unique names.

    Returns
    -------
    files : `dict` [`str`, `str`]
        Files which need to be copied to a workflow staging area.

    Raises
    ------
    NotImplementedError
        Raised if final job in GenericWorkflow is itself a workflow.
    TypeError
        Raised if final job in GenericWorkflow is invalid type.
    """
    files = {}

    # If final job exists in generic workflow, create DAG final job
    final = generic_workflow.get_final()
    if final:
        if isinstance(final, GenericWorkflow):
            raise NotImplementedError("PanDA plugin does not support a workflow as the final job")

        if not isinstance(final, GenericWorkflowJob):
            raise TypeError(f"Invalid type for GenericWorkflow.get_final() results ({type(final)})")

        dag_final_work, files = _make_doma_work(
            config,
            generic_workflow,
            final,
            task_count,
            task_chunk,
        )
        pseudo_filename = "pure_pseudoinput+qgraphNodeId:+qgraphId:"
        dag_final_work.dependency_map.append(
            {"name": pseudo_filename, "submitted": False, "dependencies": []}
        )
        idds_client_workflow.add_work(dag_final_work)
        conditions = []
        for work in dag_sink_work:
            conditions.append(work.is_terminated)
        and_cond = AndCondition(conditions=conditions, true_works=[dag_final_work])
        idds_client_workflow.add_condition(and_cond)
    else:
        _LOG.debug("No final job in GenericWorkflow")
    return files


def convert_exec_string_to_hex(cmdline):
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


def add_decoder_prefix(config, cmd_line, distribution_path, files):
    """Compose the command line sent to the pilot from the functional part
    (the actual SW running) and the middleware part (containers invocation)

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        Configuration information
    cmd_line : `str`
        UTF-8 based functional part of the command line
    distribution_path : `str`
        URI of path where all files are located for distribution
    files : `tuple` [`dict` [`str`, `str`], `list` [`str`]]
        File names needed for a task (copied local, direct access)

    Returns
    -------
    decoder_prefix : `str`
        Full command line to be executed on the edge node
    """
    # Manipulate file paths for placement on cmdline
    files_plc_hldr = {}
    for key, pfn in files[0].items():
        if pfn.endswith("/"):
            files_plc_hldr[key] = os.path.basename(pfn[:-1])
            isdir = True
        else:
            files_plc_hldr[key] = os.path.basename(pfn)
            _, extension = os.path.splitext(pfn)
            isdir = os.path.isdir(pfn) or (key == "butlerConfig" and extension != "yaml")
        if isdir:
            # this is needed to make isdir function working
            # properly in ButlerURL instance on the egde node
            files_plc_hldr[key] += "/"
        _LOG.debug("files_plc_hldr[%s] = %s", key, files_plc_hldr[key])

    cmdline_hex = convert_exec_string_to_hex(cmd_line)
    _, runner_command = config.search("runnerCommand", opt={"replaceEnvVars": False, "expandEnvVars": False})
    runner_command = runner_command.replace("\n", " ")
    decoder_prefix = runner_command.replace(
        "_cmd_line_",
        str(cmdline_hex)
        + " ${IN/L} "
        + distribution_path
        + "  "
        + "+".join(f"{k}:{v}" for k, v in files_plc_hldr.items())
        + " "
        + "+".join(files[1]),
    )
    return decoder_prefix


def add_idds_work(config, generic_workflow, idds_workflow):
    """Convert GenericWorkflowJobs to iDDS work and add them to the iDDS
        workflow.

    Parameters
    ----------
    config : `lsst.ctrl.bps.BpsConfig`
        BPS configuration
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Generic workflow containing jobs to convert.
    idds_workflow : `idds.workflowv2.workflow.Workflow`
        iDDS workflow to which the converted jobs should be added.

    Returns
    -------
    files_to_pre_stage : `dict` [`str`, `str`]
        Files that need to be copied to the staging area before submission.
    dag_sink_work : `list` [`idds.doma.workflowv2.domapandawork.DomaPanDAWork`]
        The work nodes in the client workflow which have no successors.
    task_count : `int`
        Number of tasks in iDDS workflow used for unique task names

    Raises
    ------
    RuntimeError
        If cannot recover from dependency issues after pass through workflow.
    """
    # Limit number of jobs in single PanDA task
    _, max_jobs_per_task = config.search("maxJobsPerTask", opt={"default": PANDA_DEFAULT_MAX_JOBS_PER_TASK})

    files_to_pre_stage = {}
    dag_sink_work = []  # Workflow sink nodes that need to be connected to final task
    job_to_task = {}
    job_to_pseudo_filename = {}
    task_count = 0  # Task number/ID in idds workflow used for unique name

    # To avoid dying due to optimizing number of times through workflow,
    # catch dependency issues to loop through again later.
    jobs_with_dependency_issues = {}

    # Assume jobs with same label share config values
    for job_label in generic_workflow.labels:
        _LOG.debug("job_label = %s", job_label)
        # Add each job with a particular label to a corresponding PanDA task
        # A PanDA task has a limit on number of jobs, so break into multiple
        # PanDA tasks if needed.
        job_count = 0  # Number of jobs in idds task used for task chunking
        task_chunk = 1  # Task chunk number within job label used for unique name
        work = None

        # Instead of changing code to make chunks up front and round-robin
        # assign jobs to chunks, for now keeping chunk creation in loop
        # but using knowledge of how many chunks there will be to set better
        # maximum number of jobs in a chunk for more even distribution.
        jobs_by_label = generic_workflow.get_jobs_by_label(job_label)
        num_chunks = -(-len(jobs_by_label) // max_jobs_per_task)  # ceil
        max_jobs_per_task_this_label = -(-len(jobs_by_label) // num_chunks)
        _LOG.debug(
            "For job_label = %s, num jobs = %s, num_chunks = %s, max_jobs = %s",
            job_label,
            len(jobs_by_label),
            num_chunks,
            max_jobs_per_task_this_label,
        )
        for gwjob in jobs_by_label:
            job_count += 1
            if job_count > max_jobs_per_task_this_label:
                job_count = 1
                task_chunk += 1

            if job_count == 1:
                # Create new PanDA task object
                task_count += 1
                work, files = _make_doma_work(config, generic_workflow, gwjob, task_count, task_chunk)
                files_to_pre_stage.update(files)
                idds_workflow.add_work(work)
                if generic_workflow.out_degree(gwjob.name) == 0:
                    dag_sink_work.append(work)

            pseudo_filename = _make_pseudo_filename(config, gwjob)
            job_to_pseudo_filename[gwjob.name] = pseudo_filename
            job_to_task[gwjob.name] = work.get_work_name()
            deps = []
            missing_deps = False
            for parent_job_name in generic_workflow.predecessors(gwjob.name):
                if parent_job_name not in job_to_task:
                    _LOG.debug("job_to_task.keys() = %s", job_to_task.keys())
                    missing_deps = True
                    break
                else:
                    deps.append(
                        {
                            "task": job_to_task[parent_job_name],
                            "inputname": job_to_pseudo_filename[parent_job_name],
                            "available": False,
                        }
                    )
            if not missing_deps:
                work.dependency_map.append({"name": pseudo_filename, "dependencies": deps})
            else:
                jobs_with_dependency_issues[gwjob.name] = work

    # If there were any issues figuring out dependencies through earlier loop
    if jobs_with_dependency_issues:
        _LOG.warning("Could not prepare workflow in single pass.  Please notify developers.")
        _LOG.info("Trying to recover...")
        for job_name, work in jobs_with_dependency_issues.items():
            deps = []
            for parent_job_name in generic_workflow.predecessors(job_name):
                if parent_job_name not in job_to_task:
                    _LOG.debug("job_to_task.keys() = %s", job_to_task.keys())
                    raise RuntimeError(
                        "Could not recover from dependency issues ({job_name} missing {parent_job_name})."
                    )
                deps.append(
                    {
                        "task": job_to_task[parent_job_name],
                        "inputname": job_to_pseudo_filename[parent_job_name],
                        "available": False,
                    }
                )
            pseudo_filename = job_to_pseudo_filename[job_name]
            work.dependency_map.append({"name": pseudo_filename, "dependencies": deps})
        _LOG.info("Successfully recovered.")

    return files_to_pre_stage, dag_sink_work, task_count

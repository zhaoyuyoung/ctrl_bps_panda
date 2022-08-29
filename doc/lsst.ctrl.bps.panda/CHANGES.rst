lsst-ctrl-bps-panda v24.0.0 (2022-08-29)
========================================

New Features
------------

- This package has been extracted from ``lsst_ctrl_bps`` into a standalone package to make it easier to manage development of the PanDA plugin.
  (`DM-33521 <https://jira.lsstcorp.org/browse/DM-33521>`_)
- Introduced a new parameter ``dockerImageLocation`` in the PanDA IDF configuration yaml file to pull lsst release containers from **GAR (Google Artifact Registry)**. This parameter is trailed with ``'/'``, so it could be used in ``sw_image`` path in the following example. And the ``sw_image`` will still refer to the **Docker hub**, if the parameter ``dockerImageLocation`` is empty or not defined, to make the ``sw_image`` backward compatible with previous PanDA IDF configuration yaml files.

  In the user bps submission yaml file, just prepend this parameter to the sw_image path, that is:

  .. code-block:: YAML

     sw_image: "{dockerImageLocation}lsstsqre/centos:7-stack-lsst_distrib-w_2022_05"

  Please note that there is no extra character(s) between ``{dockerImageLocation}`` and ``lsstsqre``.

  In case you have to use images from the Docker hub instead, you just take out the prefix ``{dockerImageLocation}`` in the path, that is:

  .. code-block:: YAML

     sw_image: "lsstsqre/centos:7-stack-lsst_distrib-w_2022_05" (`DM-32992 <https://jira.lsstcorp.org/browse/DM-32992>`_)

Bug Fixes
---------

- Update the path to the command line decoder in the config file and the documentation. (`DM-34574 <https://jira.lsstcorp.org/browse/DM-34574>`_)


Other Changes and Additions
---------------------------

- Changed the parameter ``runnerCommand`` in the PanDA IDF example yaml file, to start ``prmon`` to monitor the memory usage of the payload job.
  This executable ``prmon`` is only available in releases after ``w_2022_05``. (`DM-32579 <https://jira.lsstcorp.org/browse/DM-32579>`_)
- Make the PanDA example config more easily runnable from data-int RSP (`DM-32695 <https://jira.lsstcorp.org/browse/DM-32695>`_)

- * PanDA cloud was mapped from BPS compute site, fixed it.
  * Pass BPS cloud to PanDA cloud.
  * Add supports for task priority, vo, working group, prodSourceLabel. (`DM-33889 <https://jira.lsstcorp.org/browse/DM-33889>`_)
- Remove ``iddsServer`` from ``bps_idf.yml``, to use the iDDS server defined in the PanDA relay service.
   Remove ``IDDS_CONFIG`` requirements (requiring ``idds`` version 0.10.6 and later). (`DM-34106 <https://jira.lsstcorp.org/browse/DM-34106>`_)
- Add missing ``__all__`` statement to make the documentation render properly at https://pipelines.lsst.io. (`DM-34921 <https://jira.lsstcorp.org/browse/DM-34921>`_)

ctrl_bps v23.0.1 (2022-02-02)
=============================

New Features
------------

- * Large tasks (> 30k jobs) splitted into chunks
  * Updated iDDS API usage for the most recent version
  * Updated iDDS API initialization to force PanDA proxy using the IAM user name for submitted workflow
  * Added limit on number of characters in the task pseudo inputs (`DM-32675 <https://jira.lsstcorp.org/browse/DM-32675>`_)
- * New ``panda_auth`` command for handling PanDA authentication token.
    Includes status, reset, and clean capabilities.
  * Added early check of PanDA authentication token in submission process. (`DM-32830 <https://jira.lsstcorp.org/browse/DM-32830>`_)

Other Changes and Additions
---------------------------

- * Changed printing of submit directory early.
  * Changed PanDA plugin to only print the numeric id when outputing the request/run id.
  * Set maximum number of jobs in a PanDA task (maxJobsPerTask) to 70000 in config/bps_idf.yaml. (`DM-32830 <https://jira.lsstcorp.org/browse/DM-32830>`_)

ctrl_bps v23.0.0 (2021-12-10)
=============================

Other Changes and Additions
---------------------------

- Provide a cleaned up version of default config yaml for PanDA-plugin on IDF (`DM-31476 <https://jira.lsstcorp.org/browse/DM-31476>`_)

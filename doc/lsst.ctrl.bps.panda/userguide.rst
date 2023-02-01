.. _panda-plugin-overview:

Overview
--------

LSST Batch Processing Service (BPS) allow large-scale workflows to execute in
well-managed fashion, potentially in multiple environments. The service is
provided by `ctrl_bps`_ package. ``ctrl_bps_panda`` is a plugin allowing `ctrl_bps`_
to execute workflows on computational resources managed by `PanDA`_.

.. note::

   Documentation describing how to submit workflows for execution using PanDA
   deployment in Interim Data Facility (*aka* Google cloud) can can be found at
   https://panda.lsst.io.

.. _panda-plugin-wmsclass:

Specifying the plugin
---------------------

The class providing `PanDA`_ support for `ctrl_bps`_ is ::

    lsst.ctrl.bps.htcondor.PanDAService

Inform `ctrl_bps`_ about its location using one of the methods described in its
`documentation`__.

.. __: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/index.html

 .. _htc-plugin-defining-submission:

.. _panda-plugin-defining-submission:

Defining a submission
---------------------

BPS configuration files are YAML files with some reserved keywords and some
special features. See `BPS configuration file`__ for details.

.. Describe any plugin specific ascpects of a definiing a submissinon below if
   any.

The memory autoscaling is *not* supported supported by the ``ctrl_bps_panda``, i.e.,
configuration settings like:

* ``requestMemoryMax``,
* ``executeMachinesPattern``,
* ``memoryMultiplier``,
* ``memoryLimit``,

will have not effect on workflows submitted with this plugin.

.. _panda-plugin-authenticating:

Authenticating
--------------

.. Describe any plugin specific ascpects of a authentication below if any.

See https://panda.lsst.io for details.

.. _panda-plugin-submitting:

Submitting a run
----------------

See `bps submit`_ and https://panda.lsst.io for details.

.. Describe any plugin specific ascpects of a submissinon below if any.

.. __: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#submitting-a-run

.. _panda-plugin-status:

Checking status
---------------

`bps report`_ is *not* supported, use the WMS commands/tools directly.

.. Describe any plugin specific ascpects of a checking submission status below
   if any.

.. _panda-plugin-cancelling:

Canceling submitted jobs
------------------------

`bps cancel`_ is *not* supported, use the WMS commands/tools directly.

.. Describe any plugin specific ascpects of a canceling submitted jobs below
   if any.

.. _panda-plugin-restarting:

Restarting a failed run
-----------------------

`bps restart`_ is *not* supported, use the WMS commands/tools directly.

.. Describe any plugin specific ascpects of restarting a failed jobs below
   if any.

.. .. _panda-plugin-troubleshooting:

.. Troubleshooting
.. ---------------

.. _PanDA: https://panda-wms.readthedocs.io/en/latest/
.. _bps cancel: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#canceling-submitted-jobs
.. _bps report: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#checking-status
.. _bps restart: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#restarting-a-failed-run
.. _bps submit: https://pipelines.lsst.io/v/weekly/modules/lsst.ctrl.bps/quickstart.html#submitting-a-run
.. _ctrl_bps: https://github.com/lsst/ctrl_bps.git

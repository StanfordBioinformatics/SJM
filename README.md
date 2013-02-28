SJM
===

Simple Job Manager

Simple Job Manager (SJM)
========================

Summary
=======

SJM is a program for managing a group of related jobs running on a
compute cluster.  It provides a convenient method for specifying
dependencies between jobs and the resource requirements for each job
(e.g. memory, CPU cores). It monitors the status of the jobs so you
can tell when the whole group is done.  If any of the jobs fails
(e.g. due to a compute node crashing) SJM allows you to resume without
rerunning the jobs that completed successfully.  Finally, SJM provides
a portable way to submit jobs to different job schedulers such as Sun
Grid Engine or Platform LSF.

Software Requirements
=====================

Installing SJM requires the following prerequisites:

1. GCC to compile the program.

2. The Boost regex library (http://www.boost.org).  If you don't
already have it on your system you must install boost before
installing SJM.  The easiest way is to install a prebuilt package, but
you can download the source release from the boost web site and build
it yourself.  See the instructions here:
http://www.boost.org/doc/libs/1_49_0/more/getting_started/index.html
Be sure to follow the build instructions in the section called
"Prepare to Use a Boost Library Binary".

3. The TCLAP library (http://tclap.sourceforge.net/).  Download and
install it before installing SJM.

4. A job scheduler: either Sun Grid Engine or Platform LSF.  SJM
doesn't currently support other schedulers but it is relatively easy
to add a new one (see Other Job Schedulers below).

5. For Sun Grid Engine: a parallel environment for running multi-core
(threaded) jobs on a single node of the cluster.  Common names for
this PE are "smp" or "shm".  Here is a sample SGE configuration:

  $ qconf -sp smp
  pe_name            smp
  slots              999
  user_lists         NONE
  xuser_lists        NONE
  start_proc_args    NONE
  stop_proc_args     NONE
  allocation_rule    $pe_slots
  control_slaves     FALSE
  job_is_first_task  TRUE
  urgency_slots      min
  accounting_summary FALSE

6. Environment modules (optional).  SJM has optional features to
integrate with environment modules, a system for managing the user
environment (see http://modules.sourceforge.net/).  It is a convenient
way for users to switch between different versions of a software
package and to use software packages installed in non-standard
directories.

Installation
============

First download the release.  Then unpack it:

  $ tar xvzf scg-1.2.0.tgz

Finally, to compile and install sjm into /usr/local/bin run:

  $ cd scg-1.2.0
  $ ./configure
  $ make
  $ sudo make install

That's it unless you need to customize the installation.  The rest
of this section describes various additional options you can use.

To install in a different location use the --prefix option:

  $ ./configure --prefix=/destination/directory

The configure script will try to identify the job scheduler on your
system, but you can force it to use a particular scheduler using the
--with-scheduler option.  Supported values include SGE and LSF:

  $ ./configure --with-scheduler=SGE

or

  $ ./configure --with-scheduler=LSF

For Sun Grid Engine you must specify the parallel environment for
multi-core jobs using the --with-pe option:

  $ ./configure --with-pe=smp

If you wish to disable the environment modules integration then use
the flag --without-modules:

  $ ./configure --without-modules

If the boost library and/or job scheduler library and header files are
installed in a non-standard place you need to add compiler options to
find these files.  Use CPPFLAGS to specify header file locations:

  $ ./configure CPPFLAGS="-I/boost/include/dir -I/scheduler/include/dir"

Use LDFLAGS to specify library locations:

  $ ./configure LDFLAGS="-L/boost/lib/dir -L/scheduler/lib/dir"

Note that if you use shared libraries and they are in a non-standard
location then you may have to set LD_LIBRARY_PATH prior to running
sjm.

To see all of the available options run:

  $ ./configure --help

To build from a git repository, you must have GNU autoconf and
automake installed.  First run the following command and then continue
with the instructions above:

  $ sh bootstrap.sh

Usage
=====

See doc/MANUAL.txt for usage instructions.  You can run a quick test
like this:

  $ sjm -i doc/example.sjm

Note that the integration with environment modules requires a helper
script called run_with_env that is installed as part of the package.
The installation directory is hardwired into the sjm binary at compile
time and must be the same on your compute nodes.  If you need to
override that value you can set the environment variable RUN_WITH_ENV
to the full path of the script prior to running sjm. For example:

  $ export RUN_WITH_ENV=/full/path/for/run_with_env
  $ sjm ...

Other Job Schedulers
====================

If you want to use SJM with a job scheduler that isn't currently
supported you will need to write a C++ adapter class.  Use src/sge.hh
and src/sge.cc as an example.  Then add references to the new adapter
class in configure.ac, src/Makefile.am and src/job_mon.hh.  Please
contribute your changes so other people can benefit!

License
=======

SJM is distributed under the BSD 3-clause licence.  See COPYING for
the full license.

Contact
=======

Please send comments, suggestions, bug reports and bug fixes to
lacroute@users.sourceforge.net.

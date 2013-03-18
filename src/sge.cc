//
// sge.cc - sjm interface to Sun Grid Engine job submission API
//
// Phil Lacroute
// January 2010
//
// Copyright(c) 2010-2012 The Board of Trustees of The Leland Stanford
// Junior University.  All Rights Reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
// 
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
// 
//     * Redistributions in binary form must reproduce the above
//       copyright notice, this list of conditions and the following
//       disclaimer in the documentation and/or other materials provided
//       with the distribution.
// 
//     * Neither the name of Stanford University nor the names of its
//       contributors may be used to endorse or promote products derived
//       from this software without specific prior written permission.
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL STANFORD
// UNIVERSITY BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

static char const rcsid[] = "$Id: sge.cc 214 2008-07-09 21:12:42Z lacroute $";

#include <string>
#include <sstream>
#include <stdexcept>

extern "C" {
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <string.h>
}

using std::string;
using std::ostringstream;
using std::runtime_error;

#include "../config.h"
#include "sge.hh"

SgeBatchSystem::SgeBatchSystem ()
{
}

void
SgeBatchSystem::init ()
{
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    errnum = drmaa_init(NULL, error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA initialization failed: " << error;
        throw runtime_error(err.str());
    }
}

bool
SgeBatchSystem::submit (Job& job)
{
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    drmaa_job_template_t *jt = NULL;

    errnum = drmaa_allocate_job_template(&jt, error,
                                         DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA job template creation failed: " << error;
        throw runtime_error(err.str());
    }

    errnum = drmaa_set_attribute(jt, DRMAA_JOB_NAME, job.name().c_str(),
                                 error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA job cannot set job name: " << error;
        throw runtime_error(err.str());
    }

    errnum = drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh",
                                 error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA job cannot set remote command: " << error;
        throw runtime_error(err.str());
    }

    string command(job.augmentedCommand());
    for (string::iterator iter = command.begin(); iter != command.end();
	 ++iter) {
	if (*iter == '\n' || *iter =='\r') {
	    *iter = ' ';
	}
    }
    const char *args[3] = {"-c", command.c_str(), NULL};
    errnum = drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, args,
                                        error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA job cannot set command arguments: " << error;
        throw runtime_error(err.str());
    }

    ostringstream qparams;
    qparams << "-w e -b y";
    if (job.memoryLimit() > 0) {
    	qparams << " -l h_vmem=" << job.memoryLimit()  << "M";
    }
    if (job.timeLimit() > 0) {
	qparams << " -l h_rt=" << job.timeLimit() << ":00:00";
    }
    if (!job.queue().empty()) {
	qparams << " -q " << job.queue();
    }
    if (!job.project().empty()) {
	qparams << " -P " << job.project();
    }
    for (Job::EnvVarList::const_iterator iter = job.exportVars().begin();
	 iter != job.exportVars().end(); ++iter) {
	qparams << " -v " << iter->first;
	if (!iter->second.empty()) {
	    setenv(iter->first.c_str(), iter->second.c_str(), 1);
	}
    }
    if (!job.modules().empty()) {
	qparams << " -v MODULESHOME -v MODULEPATH";
    }
    if (!job.directory().empty()) {
	qparams << " -wd " << job.directory();
    }
    if (job.slots() != 0) {
	qparams << " -pe ";
	if (job.parallelEnv().empty()) {
	    qparams << SGE_MULTICORE_PE;
	} else {
	    qparams << job.parallelEnv();
	}
	qparams << " " << job.slots() << " -R y";
    }
    if (!job.schedOptions().empty()) {
	qparams << " " << job.schedOptions();
    }
	
    errnum = drmaa_set_attribute(jt, DRMAA_NATIVE_SPECIFICATION,
				 qparams.str().c_str(),
                                 error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "cannot set parameters for job " << job.name() << ": " << error;
        throw runtime_error(err.str());
    }

    if (!job.jobGraph().logDir().empty()) {
	string ofile;
	ostringstream ofilespec;
	job.stdoutLogFilename(ofile);
	ofilespec << ":" << ofile;
	errnum = drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH,
				     ofilespec.str().c_str(),
				     error, DRMAA_ERROR_STRING_BUFFER);
	if (errnum != DRMAA_ERRNO_SUCCESS) {
	    ostringstream err;
	    err << "cannot set stdout for job " << job.name() << ": " << error;
	    throw runtime_error(err.str());
	}

	string efile;
	ostringstream efilespec;
	job.stderrLogFilename(efile);
	efilespec << ":" << efile;
	errnum = drmaa_set_attribute(jt, DRMAA_ERROR_PATH,
				     efilespec.str().c_str(),
				     error, DRMAA_ERROR_STRING_BUFFER);
	if (errnum != DRMAA_ERRNO_SUCCESS) {
	    ostringstream err;
	    err << "cannot set stderr for job " << job.name() << ": " << error;
	    throw runtime_error(err.str());
	}
    }

    char jobid[DRMAA_JOBNAME_BUFFER];
    errnum = drmaa_run_job(jobid, DRMAA_JOBNAME_BUFFER, jt, error,
                           DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "cannot submit job " << job.name() << ": " << error;
        throw runtime_error(err.str());
    }

    errnum = drmaa_delete_job_template(jt, error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA: cannot delete job template: " << error;
        throw runtime_error(err.str());
    }

    job.setJobId(atoi(jobid));
    job.changeStatus(Job::Dispatched);

    return true;
}

void
SgeBatchSystem::kill (Job& job)
{
    char jobid[DRMAA_JOBNAME_BUFFER];
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;

    if (job.status() == Job::Dispatched ||
	job.status() == Job::Pending ||
	job.status() == Job::Running) {
	sprintf(jobid, "%llu", job.jobId());
	errnum = drmaa_control(jobid, DRMAA_CONTROL_TERMINATE, error,
			       DRMAA_ERROR_STRING_BUFFER);
	if (errnum != DRMAA_ERRNO_SUCCESS) {
	    ostringstream err;
	    err << "cannot kill job " << job.name() << ": " << error;
	    throw runtime_error(err.str());
	}
	job.changeStatus(Job::Failed);
    }
}

bool
SgeBatchSystem::checkStatus (Job& job)
{
    char error[DRMAA_ERROR_STRING_BUFFER];
    char job_id[DRMAA_JOBNAME_BUFFER];
    int errnum, status;

    sprintf(job_id, "%llu", job.jobId());
    errnum = drmaa_job_ps(job_id, &status, error, DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "cannot check job " << job.name() << ": " << error;
	throw runtime_error(err.str());
    }

    bool newState = false;
    bool exception = false;
    switch (status) {
    case DRMAA_PS_RUNNING:
	if (job.status() == Job::Dispatched ||
	    job.status() == Job::Pending) {
	    job.changeStatus(Job::Running);
	    newState = true;
	} else if (job.status() != Job::Running) {
	    exception = true;
	}
	break;
    case DRMAA_PS_QUEUED_ACTIVE:
	if (job.status() == Job::Dispatched) {
	    job.changeStatus(Job::Pending);
	    newState = true;
	} else if (job.status() != Job::Pending) {
	    exception = true;
	}
	break;
    case DRMAA_PS_SYSTEM_ON_HOLD:
    case DRMAA_PS_USER_ON_HOLD:
    case DRMAA_PS_USER_SYSTEM_ON_HOLD:
    case DRMAA_PS_SYSTEM_SUSPENDED:
    case DRMAA_PS_USER_SUSPENDED:
    case DRMAA_PS_USER_SYSTEM_SUSPENDED:
    case DRMAA_PS_UNDETERMINED:
	exception = true;
	break;
    case DRMAA_PS_DONE:
    case DRMAA_PS_FAILED:
	// change the job state when the job is reaped by wait(), not here
	break;
    default:
	ostringstream err;
	err << "cannot check job " << job.name() << ": unexpected status "
	    << status;
	throw runtime_error(err.str());
    }
    if (exception != job.hasException()) {
	job.setException(exception);
	newState = true;
    }

    return newState;
}

bool
SgeBatchSystem::canWait ()
{
    // SGE can wait for jobs to complete
    return true;
}

// wait until a job finishes or the timeout expires, whichever comes first;
// if the timeout is 0 then return immediately even if no job has finished
Job *
SgeBatchSystem::wait (JobGraph& jobs, unsigned timeout_sec)
{
    char jobid[DRMAA_JOBNAME_BUFFER];
    char error[DRMAA_ERROR_STRING_BUFFER];
    int errnum = 0;
    int job_status = 0;
    drmaa_attr_values_t *rusage = 0;
    Job *job;

    errnum = drmaa_wait(DRMAA_JOB_IDS_SESSION_ANY, jobid,
			DRMAA_JOBNAME_BUFFER, &job_status,
			timeout_sec, &rusage, error,
			DRMAA_ERROR_STRING_BUFFER);
    if (errnum == DRMAA_ERRNO_EXIT_TIMEOUT) {
	// no jobs are ready but the timeout expired, so return
	return 0;
    }
    if (errnum == DRMAA_ERRNO_INVALID_JOB) {
	// there are no submitted jobs, so just wait until the timeout expires
	if (timeout_sec > 0) {
	    sleep(timeout_sec);
	}
	return 0;
    }
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "cannot wait for jobs: " << error;
	throw runtime_error(err.str());
    }
    job = jobs.find(atol(jobid));
    if (job == 0) {
	ostringstream err;
	err << "wait returned invalid job " << jobid;
	throw runtime_error(err.str());
    }
    job->setException(false); // clear any old exceptions

    int aborted = 0;
    drmaa_wifaborted(&aborted, job_status, error,
		     DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "cannot determine if job " << job->name() << " aborted: "
	    << error;
	throw runtime_error(err.str());
    }
    if (aborted == 1) {
	if (job->status() == Job::Dispatched ||
	    job->status() == Job::Pending ||
	    job->status() == Job::Running) {
	    job->changeStatus(Job::Failed);
	}
	return job;
    }

    int exited = 0;
    drmaa_wifexited(&exited, job_status, error,
		    DRMAA_ERROR_STRING_BUFFER);
    if (errnum != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "cannot determine if job " << job->name() << " exited: "
	    << error;
	throw runtime_error(err.str());
    }
    if (exited == 1) {
	int exitCode = 0;
	drmaa_wexitstatus(&exitCode, job_status, error,
			  DRMAA_ERROR_STRING_BUFFER);
	if (errnum != DRMAA_ERRNO_SUCCESS) {
	    ostringstream err;
	    err << "cannot get exit status for job " << job->name() << ": "
		<< error;
	    throw runtime_error(err.str());
	}
	if (job->status() == Job::Dispatched ||
	    job->status() == Job::Pending ||
	    job->status() == Job::Running) {
	    if (exitCode == 0) {
		job->changeStatus(Job::Done);
	    } else {
		job->changeStatus(Job::Failed);
	    }
	}
	job->setExitCode(exitCode);
    } else {
	job->changeStatus(Job::Failed);
    }

    char usage[DRMAA_ERROR_STRING_BUFFER];
    while (drmaa_get_next_attr_value(rusage, usage,
				     DRMAA_ERROR_STRING_BUFFER) ==
	   DRMAA_ERRNO_SUCCESS) {
	double value;
	if (parseUsageString(usage, "ru_wallclock", &value)) {
	    job->setWallclockTime((unsigned)value);
	} else if (parseUsageString(usage, "cpu", &value)) {
	    job->setCpuTime((unsigned)value);
	} else if (parseUsageString(usage, "maxvmem", &value)) {
	    job->setMemoryUsage((unsigned)(value / 1000000));
	}
    }
    drmaa_release_attr_values(rusage);

    return job;
}

bool
SgeBatchSystem::parseUsageString (const char* usage, const char* name,
				  double* value)
{
    const char* usage_ptr = usage;
    const char* name_ptr = name;
    for (; *name_ptr != '\0' && *usage_ptr != '\0'; ++name_ptr, ++usage_ptr) {
	if (*usage_ptr != *name_ptr) {
	    return false;
	}
    }
    if (*name_ptr != '\0' || *usage_ptr != '=') {
	return false;
    }
    ++usage_ptr;
    char *end_ptr;
    *value = strtod(usage_ptr, &end_ptr);
    if (end_ptr == usage_ptr) {
	ostringstream err;
	err << "cannot parse resource usage value for " << name
	    << ": " << usage_ptr;
	throw runtime_error(err.str());
    }
    return true;
}

void
SgeBatchSystem::cleanup ()
{
    char error[DRMAA_ERROR_STRING_BUFFER];
    if (drmaa_exit(error, sizeof(error)-1) != DRMAA_ERRNO_SUCCESS) {
	ostringstream err;
	err << "SGE DRMAA exit failed: " << error;
        throw runtime_error(err.str());
    }
}

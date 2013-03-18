//
// lsf.cc - sjm interface to LSF job submission API
//
// Phil Lacroute
// March 2008
//
// Copyright(c) 2008-2012 The Board of Trustees of The Leland Stanford
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

static char const rcsid[] = "$Id: lsf.cc 214 2008-07-09 21:12:42Z lacroute $";

#include <string>
#include <sstream>
#include <stdexcept>

using std::string;
using std::ostringstream;
using std::runtime_error;

#include "lsf.hh"

LsfBatchSystem::LsfBatchSystem ()
{
}

void
LsfBatchSystem::init ()
{
    if (setenv("BSUB_QUIET", "1", 1) < 0) {
	throw runtime_error("setenv failed");
    }
    if (lsb_init("sjm") < 0) {
	ostringstream err;
	err << "LSF initialization failed: "
	    << lsb_sysmsg() << " (" << lsberrno << ")";
	throw runtime_error(err.str());
    }
}

bool
LsfBatchSystem::submit (Job& job)
{
    struct submit req;
    memset(&req, 0, sizeof(req));

    req.options |= SUB_JOB_NAME;
    req.jobName = const_cast<char *>(job.name().c_str());
    req.numProcessors = 1;
    req.maxNumProcessors = 1;
    req.command = const_cast<char *>(job.command().c_str());
    for (unsigned idx = 0; idx < LSF_RLIM_NLIMITS; ++idx) {
	req.rLimits[idx] = DEFAULT_RLIMIT;
    }
    req.userPriority = -1;
    req.warningTimePeriod = -1;

    string resReq;
    if (job.memoryLimit() > 0) {
	req.rLimits[LSF_RLIMIT_RSS] = job.memoryLimit() * 1000;
	req.options |= SUB_RES_REQ;
	ostringstream resReqStream;
	resReqStream << "rusage[mem=" << job.memoryLimit() << "]";
	resReq.assign(resReqStream.str());
	req.resReq = const_cast<char *>(resReq.c_str());
    }

    if (job.timeLimit() > 0) {
	req.rLimits[LSF_RLIMIT_RUN] = job.timeLimit() * 60 * 60;
    }

    if (!job.queue().empty()) {
	req.options |= SUB_QUEUE;
	req.queue = const_cast<char *>(job.queue().c_str());
    }

    struct submitReply reply;
    LS_LONG_INT jobId = lsb_submit(&req, &reply);
    if (jobId < 0) {
	if (lsberrno == LSBE_PJOB_LIMIT) {
	    return false;
	}
	ostringstream err;
	err << "cannot submit job " << job.name() << ": "
	    << lsb_sysmsg() << " (" << lsberrno << ")";
	throw runtime_error(err.str());
    }

    job.setJobId(jobId);
    job.changeStatus(Job::Dispatched);

    return true;
}

void
LsfBatchSystem::kill (Job& job)
{
    if (job.status() == Job::Dispatched ||
	job.status() == Job::Pending ||
	job.status() == Job::Running) {
	if (lsb_signaljob(job.jobId(), SIGKILL) < 0) {
	    ostringstream err;
	    err << "cannot kill job " << job.name() << ": "
		<< lsb_sysmsg() << " (" << lsberrno << ")";
	    throw runtime_error(err.str());
	}
	job.changeStatus(Job::Failed);
    }
}

bool
LsfBatchSystem::checkStatus (Job& job)
{
    int count = lsb_openjobinfo(job.jobId(), 0, 0, 0, 0, ALL_JOB);
    if (count < 0) {
	ostringstream err;
	err << "cannot check job " << job.name() << ": "
	    << lsb_sysmsg() << " (" << lsberrno << ")";
	throw runtime_error(err.str());
    }
    if (count != 1) {
	ostringstream err;
	err << "cannot check job " << job.name()
	    << ": expected one record but got " << count;
	throw runtime_error(err.str());
    }
    int more = 0;
    struct jobInfoEnt *jobInfo = lsb_readjobinfo(&more);
    if (jobInfo == 0) {
	ostringstream err;
	err << "cannot check job " << job.name() << ": missing record";
	throw runtime_error(err.str());
    }
    if ((Job::JobId)jobInfo->jobId != job.jobId()) {
	ostringstream err;
	err << "cannot check job " << job.name() << ": expected job id "
	    << job.jobId() << " but got " << jobInfo->jobId;
	throw runtime_error(err.str());
    }

    bool newState = false;
    bool exception = false;
    if ((jobInfo->status & JOB_STAT_EXIT) ||
	((jobInfo->status & JOB_STAT_DONE) &&
	 (jobInfo->status & JOB_STAT_PERR))) {
	if (job.status() == Job::Dispatched ||
	    job.status() == Job::Pending ||
	    job.status() == Job::Running) {
	    job.changeStatus(Job::Failed);
	    job.setWallclockTime(jobInfo->endTime - jobInfo->startTime);
	    job.setExitCode(jobInfo->exitStatus);
	    newState = true;
	} else if (job.status() != Job::Failed) {
	    exception = true;
	}
    } else if (jobInfo->status & JOB_STAT_DONE) {
	if (jobInfo->status & JOB_STAT_PDONE) {
	    if (job.status() == Job::Dispatched ||
		job.status() == Job::Pending ||
		job.status() == Job::Running) {
		job.changeStatus(Job::Done);
		job.setWallclockTime(jobInfo->endTime - jobInfo->startTime);
		newState = true;
	    } else if (job.status() != Job::Done) {
		exception = true;
	    }
	}
    } else if (jobInfo->status & JOB_STAT_RUN) {
	if (job.status() == Job::Dispatched ||
	    job.status() == Job::Pending) {
	    job.changeStatus(Job::Running);
	    newState = true;
	} else if (job.status() != Job::Running) {
	    exception = true;
	}
	job.setWallclockTime(time(0) - jobInfo->startTime);
    } else if (jobInfo->status & JOB_STAT_PEND) {
	if (job.status() != Job::Dispatched && job.status() != Job::Pending) {
	    exception = true;
	} else {
	    bool newJob = false;
	    for (int idx = 0; idx < jobInfo->numReasons; ++idx) {
		if (jobInfo->reasonTb[idx] == PEND_JOB_NEW) {
		    newJob = true;
		    break;
		}
	    }
	    if (!newJob && job.status() == Job::Dispatched) {
		job.changeStatus(Job::Pending);
		newState = true;
	    }
	}
    } else if (jobInfo->status & (JOB_STAT_PSUSP | JOB_STAT_SSUSP |
				  JOB_STAT_USUSP | JOB_STAT_UNKWN)) {
	exception = true;
    } else {
	ostringstream err;
	err << "cannot check job " << job.name() << ": unexpected status "
	    << jobInfo->status;
	throw runtime_error(err.str());
    }
    if (exception != job.hasException()) {
	job.setException(exception);
	newState = true;
    }

    if ((jobInfo->status & (JOB_STAT_PEND | JOB_STAT_UNKWN)) == 0) {
	job.setCpuTime((unsigned)jobInfo->cpuTime);
	job.setMemoryUsage(jobInfo->runRusage.mem / 1000);
	job.setSwapUsage(jobInfo->runRusage.swap / 1000);
    }

    lsb_closejobinfo();

    return newState;
}

bool
LsfBatchSystem::canWait ()
{
    // LSF does not support a wait() call
    return false;
}

Job *
LsfBatchSystem::wait (JobGraph& jobs, unsigned timeout_sec)
{
    return 0;
}

void
LsfBatchSystem::cleanup ()
{
}

//
// batch.cc - sjm batch system interface
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

static char const rcsid[] = "$Id: batch.cc 171 2008-04-17 22:57:24Z lacroute $";

#include <stdexcept>
#include <iostream>
#include <sstream>

extern "C" {
#include <string.h>
#include <sys/time.h>
#include <sys/errno.h>
}

#include "batch.hh"

using std::runtime_error;
using std::logic_error;
using std::ostringstream;
using std::endl;

StubBatchSystem::StubBatchSystem ():
    nextJobId_(1)
{
}

// return true if job is submitted without problems
bool
StubBatchSystem::submit (Job& job)
{
    JobState state;
    state.submitTime = currentTime();
    unsigned jobCnt = 0;
    for (JobMap::iterator iter = jobs_.begin(); iter != jobs_.end(); ++iter) {
	if (iter->second.endTime > state.submitTime) {
	    ++jobCnt;
	}
    }
    state.schedTime = state.submitTime + 20;
    if (jobCnt > 35) {
	state.startTime = state.schedTime + 120;
    } else {
	state.startTime = state.schedTime;
    }
    state.endTime = state.startTime + 60;
    jobs_[nextJobId_] = state;
    job.setJobId(nextJobId_);
    job.changeStatus(Job::Dispatched);
    ++nextJobId_;
    return true;
}

void
StubBatchSystem::kill (Job& job)
{
    if (job.status() == Job::Dispatched ||
	job.status() == Job::Running) {
	job.changeStatus(Job::Failed);
    }
}

// return true if status changes
bool
StubBatchSystem::checkStatus (Job& job)
{
    JobMap::iterator iter = jobs_.find(job.jobId());
    if (iter == jobs_.end()) {
	throw logic_error("invalid job " + job.name());
    }
    unsigned now = currentTime();
    if (now > iter->second.endTime) {
	if (job.status() != Job::Done) {
	    job.changeStatus(Job::Done);
	    job.setCpuTime(5);
	    job.setWallclockTime(7);
	    job.setMemoryUsage(200);
	    job.setSwapUsage(250);
	    return true;
	}
    } else if (now > iter->second.startTime) {
	if (job.status() == Job::Dispatched || job.status() == Job::Pending) {
	    job.changeStatus(Job::Running);
	    return true;
	}
    } else if (now > iter->second.schedTime) {
	if (job.status() == Job::Dispatched) {
	    job.changeStatus(Job::Pending);
	    return true;
	}
    }
    return false;
}

// return true iff batch system supports the wait call
bool
StubBatchSystem::canWait ()
{
    return false;
}

// wait for a job to finish or exit
Job *
StubBatchSystem::wait (JobGraph& jobs, unsigned timeout_sec)
{
    return 0;
}

// return current time in seconds
unsigned
StubBatchSystem::currentTime ()
{
    struct timeval tv;
    if (gettimeofday(&tv, 0) < 0) {
	ostringstream err;
	err << "gettimeofday failed: " << strerror(errno);
	throw runtime_error(err.str());
    }
    return tv.tv_sec;
}

void
StubBatchSystem::cleanup ()
{
}

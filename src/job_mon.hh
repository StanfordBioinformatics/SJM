//
// job_mon.hh - sjm job monitor
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
// $Id: job_mon.hh 240 2009-03-19 21:51:09Z lacroute $
//

#ifndef JOB_MON_H
#define JOB_MON_H

#include <iostream>
#include <fstream>
#include <vector>
#include <list>
#include <string>
#include <boost/regex.hpp>

#include "job_graph.hh"
#include "batch.hh"

#if defined(HAVE_SGE)
#include "sge.hh"
#elif defined(HAVE_LSF)
#include "lsf.hh"
#endif

using std::ofstream;

class RateLimiter {
public:
    RateLimiter();
    void init(unsigned tokens, unsigned intervalSeconds,
	      unsigned currentTime);
    unsigned updateTime(unsigned currentTime);
    bool transmitOk();
    bool empty();

private:
    unsigned maxTokens_;
    unsigned intervalSeconds_;
    unsigned tokensLeft_;
    unsigned nextIncrementTime_;
};

class JobMonitor {
public:
    typedef enum {
	LogVerbose = 1,
	LogInfo,
	LogWarning,
	LogError,
	LogAlways
    } LogLevel;

    JobMonitor();
    bool submit(const std::string& jobFile, const std::string& outFile,
		const std::string& logFile, bool interactive,
		const std::vector<std::string>& mailAddr);
    bool render(const std::string& jobFile, const std::string& outFile);
    void setDispatchCount(unsigned dispatchCount);
    void setDispatchInterval(unsigned dispatchInterval);
    void setPendingJobLimit(unsigned pendingJobLimit);
    void setRunningJobLimit(unsigned runningJobLimit);
    void setCheckJobInterval(unsigned checkJobInterval);
    void setSaveInterval(unsigned saveInterval);
    void setLogInterval(unsigned logInterval);
    void setMaxStatusErr(unsigned maxStatusErr);
    void setLogLevel(LogLevel level);
    void sendEmail(const std::vector<std::string>& mailAddr,
		   const std::string& subject,
		   const std::string& message);

private:
    // default job dispatch limits
    static const unsigned defDispatchCount = 15; // max jobs per interval
    static const unsigned defPendingJobLimit = 100; // max pending jobs
    static const unsigned defMaxStatusErr = 3; // give up after this many
				// status-collection errors

    // default time intervals in seconds
    static const unsigned defDispatchInterval = 30; // dispatch interval
    static const unsigned defCheckJobInterval = 5*60; // check job status
    static const unsigned defSaveInterval = 10*60; // save job status to disk
    static const unsigned defLogInterval = 10*60; // log status message

    static const boost::regex filenameRE;

    bool verbose_;
    unsigned dispatchCount_;
    unsigned dispatchInterval_;
    unsigned pendingJobLimit_;
    unsigned runningJobLimit_;
    unsigned checkJobInterval_;
    unsigned saveInterval_;
    unsigned logInterval_;
    unsigned maxStatusErr_;
    JobGraph jobs_;		// job dependency graph
    RateLimiter dispatchRateLimiter_; // rate limiter for job dispatch
#if defined(HAVE_SGE)
    SgeBatchSystem batch_;
#elif defined(HAVE_LSF)
    LsfBatchSystem batch_;
#else
    StubBatchSystem batch_;
#endif
    unsigned startTime_;
    unsigned nextRateUpdateTime_;
    unsigned nextCheckDispatchedTime_;
    unsigned nextCheckJobTime_;
    unsigned nextSaveTime_;
    unsigned nextLogTime_;
    std::string statusFile_;
    std::string backupFile_;
    std::string logFile_;
    std::ofstream logOfs_;
    std::ostream* logOs_;
    bool interactive_;
    LogLevel logLevel_;

    void processEvents();
    bool submitJob();
    bool waitForEvent();
    void computeTimeout(unsigned nextTime, unsigned now, unsigned& timeout);
    bool checkJobs(JobGraph::JobList& jobList);
    void initFiles(const std::string& jobfile, const std::string& outFile,
		   const std::string& logFile);
    void saveStatus(bool backup);
    void logStatus();
    void collectStatus(std::ostringstream& msg);
    void printJobCount(std::ostream& os, const JobGraph::JobList& list,
		       Job::Status status, bool& first);
    void logResults();
    void logJobs(JobGraph::JobList& jobList, bool logStats);
    bool checkSignals();
    void kill();
    bool runLocalJob(Job& job);
    bool checkLocalJob(Job& job);
    void killLocalJob(Job& job);

    unsigned currentTime();
    void log(LogLevel level, const std::string& msg);
};

#endif // JOB_MON_H

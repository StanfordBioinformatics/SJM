//
// job_mon.cc - sjm job monitor
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

static char const rcsid[] = "$Id: job_mon.cc 240 2009-03-19 21:51:09Z lacroute $";

#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <iomanip>
#include <string>

extern "C" {
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/errno.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <sys/wait.h>
}

#include "../config.h"
#include "job_mon.hh"
#include "job_graph.hh"

#include <boost/algorithm/string.hpp>

using std::cin;
using std::cout;
using std::cerr;
using std::endl;
using std::flush;
using std::setw;
using std::setfill;
using std::ostream;
using std::ofstream;
using std::ostringstream;
using std::runtime_error;
using std::exception;
using std::auto_ptr;
using std::string;
using std::vector;
using boost::regex;
using boost::smatch;
using boost::regex_match;

namespace {
    bool GotInterrupt = false;
    bool GotQuit = false;

    void
    HandleInterrupt (int signal)
    {
	GotInterrupt = true;
    }

    void
    HandleQuit (int signal)
    {
	GotQuit = true;
    }

    void
    CatchSignals ()
    {
	signal(SIGINT, HandleInterrupt);
	signal(SIGQUIT, HandleQuit);
    }
}

RateLimiter::RateLimiter():
    maxTokens_(0),
    intervalSeconds_(0),
    tokensLeft_(0),
    nextIncrementTime_(0)
{
}

void
RateLimiter::init (unsigned tokens, unsigned intervalSeconds,
		   unsigned currentTime)
{
    maxTokens_ = tokens;
    intervalSeconds_ = intervalSeconds;
    tokensLeft_ = tokens;
    nextIncrementTime_ = currentTime + intervalSeconds ;
}

unsigned
RateLimiter::updateTime (unsigned currentTime)
{
    if (currentTime >= nextIncrementTime_) {
	tokensLeft_ = maxTokens_;
	nextIncrementTime_ = currentTime + intervalSeconds_;
	return nextIncrementTime_;
    } else {
	return nextIncrementTime_;
    }
}

bool
RateLimiter::transmitOk ()
{
    if (tokensLeft_ == 0) {
	return false;
    }
    --tokensLeft_;
    return true;
}

bool
RateLimiter::empty ()
{
    return tokensLeft_ == 0;
}

const regex JobMonitor::filenameRE("(.*)\\.log(\\d+)?");

JobMonitor::JobMonitor ():
    verbose_(true),
    dispatchCount_(defDispatchCount),
    dispatchInterval_(defDispatchInterval),
    pendingJobLimit_(defPendingJobLimit),
    runningJobLimit_(0),
    checkJobInterval_(defCheckJobInterval),
    saveInterval_(defSaveInterval),
    logInterval_(defLogInterval),
    maxStatusErr_(defMaxStatusErr),
    startTime_(0),
    nextRateUpdateTime_(0),
    nextCheckDispatchedTime_(0),
    nextCheckJobTime_(0),
    nextSaveTime_(0),
    nextLogTime_(0),
    interactive_(false),
    logLevel_(LogInfo)
{
}

bool
JobMonitor::submit (const string& jobFile, const string& outFile,
		    const string& logFile, bool interactive,
		    const vector<string>& mailAddr)
{
    // initialize
    interactive_ = interactive;
    try {
	jobs_.load(jobFile);
	jobs_.sanity();
	jobs_.validate();
	initFiles(jobFile, outFile, logFile);
    } catch (const runtime_error& err) {
	// print error message since log file may not be set up yet
	cerr << "sjm: " << err.what() << endl;
	return false;
    }

    // put process in background if not interactive
    if (!interactive) {
	int null_fd;
	ostringstream pid_msg;
	pid_t pid = fork();
	switch (pid) {
	default: // parent process
	    cout << "Running jobs in the background...." << endl;
	    return true;
	case 0: // child process
	    // detach from the controlling terminal
	    if (setsid() < 0) {
		cerr << "sjm: cannot create process session: "
		     << strerror(errno) << endl;
		return false;
	    }

	    // attach stdin, stdout and stderr to /dev/null
	    null_fd = open("/dev/null", O_RDWR);
	    dup2(null_fd, 0);
	    dup2(null_fd, 1);
	    dup2(null_fd, 2);

	    pid_msg << "sjm process ID: " << getpid();
	    log(LogInfo, pid_msg.str());
	    break;
	case -1: // error
	    cerr << "sjm: fork failed: " << strerror(errno) << endl;
	    return false;
	}
    }
    // do not write directly to cout or cerr beyond this point!!!

    // set the starting time to the current time
    startTime_ = currentTime();
    time_t time = startTime_;
    struct tm tm;
    localtime_r(&time, &tm);
    char now[64];
    sprintf(now, "%02d%02d%02d%02d%02d%02d",
            tm.tm_year % 100,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec);
    string runId(now);
    jobs_.setRunId(runId);

    // initialize batch system (must be done after fork)
    batch_.init();

    // run the event loop
    bool result = false;
    try {
	processEvents();
	result = jobs_.succeeded();
    } catch (const exception& err) {
	log(LogError, err.what());
	result = false;
    } catch (...) {
	log(LogError, "unknown exception");
	result = false;
    }

    // send email
    if (mailAddr.size() != 0) {

				std::vector<std::string> strs;
				boost::split(strs, jobFile, boost::is_any_of("/"));
			  ostringstream subject;
				if (strs.size() <= 1)
				{
					subject << "sjm ";
					subject << (result ? "succeeded" : "failed");
					subject << " on job file " << jobFile;
				}
				else
				{
					std::string shortPath = strs[ strs.size() -2] + "/" + strs[ strs.size() -1 ];
					subject << "sjm ";
					subject << (result ? "succeeded" : "failed");
					subject << " on job file " << shortPath;
			  }


	ostringstream message;
	message << "Job file: " << jobFile << "\n";
	if (!logFile_.empty()) {
	    message << "Log file: " << logFile_ << "\n";
	}
	message << "Summary: ";
	collectStatus(message);
	message << "\n";
	message << "Result: ";
	message << (result ? "succeeded" : "failed") << "\n";
	sendEmail(mailAddr, subject.str(), message.str());
    }

    batch_.cleanup();

    return result;
}

void
JobMonitor::processEvents ()
{
    CatchSignals();
    saveStatus(false);

    log(LogInfo, "starting");
    dispatchRateLimiter_.init(dispatchCount_, dispatchInterval_, startTime_);
    nextRateUpdateTime_ = dispatchRateLimiter_.updateTime(startTime_);
    nextCheckDispatchedTime_ = startTime_ + dispatchInterval_;
    nextCheckJobTime_ = startTime_ + checkJobInterval_;
    nextSaveTime_ = startTime_ + saveInterval_;
    nextLogTime_ = startTime_ + logInterval_;
    bool trySubmit = true;
    while (!jobs_.finished()) {
	if (!trySubmit) {
	    // wait until next timeout
	    trySubmit = waitForEvent();
	}

	// abort if there was a signal
	if (checkSignals()) {
	    break;
	}

	unsigned now = currentTime();

	// check on dispatched and running jobs
	if (nextCheckDispatchedTime_ <= now) {
	    nextCheckDispatchedTime_ = now + dispatchInterval_;
	    if (jobs_.dispatched().size() > 0) {
		log(LogVerbose, "checking dispatched jobs");
		if (checkJobs(jobs_.dispatched())) {
		    trySubmit = true;
		}
	    }
	}
	if (nextCheckJobTime_ <= now) {
	    nextCheckJobTime_ = now + checkJobInterval_;
	    if (jobs_.pending().size() + jobs_.running().size() > 0) {
		log(LogVerbose, "checking pending/running jobs");
		if (checkJobs(jobs_.pending())) {
		    trySubmit = true;
		}
		if (checkJobs(jobs_.running())) {
		    trySubmit = true;
		}
	    }
	}

	// update job-dispatch rate limiter
	if (nextRateUpdateTime_ <= now) {
	    if (dispatchRateLimiter_.empty()) {
		// try to submit again after replenishing rate limiter
		trySubmit = true;
	    }
	    nextRateUpdateTime_ = dispatchRateLimiter_.updateTime(now);
	}

	// submit one job
	if (trySubmit) {
	    trySubmit = submitJob();
	}

	// write periodic log message
	if (nextLogTime_ <= now) {
	    logStatus();
	    nextLogTime_ = now + logInterval_;
	}

	// write job state to disk
	if (nextSaveTime_ <= now) {
	    saveStatus(true);
	    nextSaveTime_ = now + saveInterval_;
	}
    }

    saveStatus(true);
    log(LogInfo, "finished");
    logResults();
}

bool
JobMonitor::submitJob ()
{
    Job* job = jobs_.firstReady();
    if (job == 0) {
	return false;
    }
    unsigned pendingCnt = jobs_.dispatched().size() + jobs_.pending().size();
    if (pendingCnt >= pendingJobLimit_) {
	log(LogVerbose, "pending job limit exceeded");
	return false;
    }
    unsigned runningCnt = jobs_.dispatched().size() +
	jobs_.pending().size() + jobs_.running().size();
    if (runningCnt >= runningJobLimit_ && runningJobLimit_ > 0) {
	log(LogVerbose, "running job limit exceeded");
	return false;
    }
    if (!dispatchRateLimiter_.transmitOk()) {
	log(LogVerbose, "dispatch rate limit exceeded");
	return false;
    }
    bool result;
    try {
	if (job->runLocally()) {
	    result = runLocalJob(*job);
	} else {
	    result = batch_.submit(*job);
	}
    } catch (const runtime_error& err) {
	log(LogError, err.what());
	job->changeStatus(Job::Failed);
	return true;
    }
    if (result) {
 	ostringstream msg;
	msg << "submitted " << job->name() << " (" << job->jobId() << ")";
	log(LogInfo, msg.str());
    } else {
	log(LogVerbose, "submit stalled");
    }
    return result;
}

bool
JobMonitor::waitForEvent ()
{
    unsigned now = currentTime();
    unsigned timeout = checkJobInterval_;
    computeTimeout(nextCheckDispatchedTime_, now, timeout);
    computeTimeout(nextCheckJobTime_, now, timeout);
    computeTimeout(nextSaveTime_, now, timeout);
    computeTimeout(nextRateUpdateTime_, now, timeout);
    computeTimeout(nextLogTime_, now, timeout);
    if (logLevel_ >= LogVerbose) {
 	ostringstream msg;
	msg << "waiting for " << timeout << " seconds";
	log(LogVerbose, msg.str());
    }
    if (batch_.canWait()) {
	Job* job = batch_.wait(jobs_, timeout);
	if (job != 0) {
	    ostringstream msg;
	    msg << "job " << job->name() << ' ' << job->statusString();
	    if (job->hasException()) {
		msg << " EXCEPTION";
	    }
	    log(LogWarning, msg.str());
	    return true;
	}
    } else if (timeout > 0) {
	sleep(timeout);
    }
    return false;
}

void
JobMonitor::computeTimeout (unsigned nextTime, unsigned now, unsigned& timeout)
{
    if (nextTime <= now) {
	timeout = 0;
    } else {
	unsigned newTimeout = nextTime - now;
	if (newTimeout < timeout) {
	    timeout = newTimeout;
	}
    }
}

bool
JobMonitor::checkJobs (JobGraph::JobList& jobList)
{
    bool changed = false;
    unsigned burstJobCnt = 0;
    const unsigned maxBurstJobCnt = 10;
    JobGraph::JobList::iterator curIter = jobList.begin();
    while (curIter != jobList.end()) {
	if (++burstJobCnt > maxBurstJobCnt) {
	    sleep(1);
	    burstJobCnt = 0;
	}

	// remember the next job on the list in case the current job
	// moves to another list
	JobGraph::JobList::iterator nextIter = curIter;
	++nextIter;

	bool newState = false;
	try {
	    if ((*curIter)->runLocally()) {
		newState = checkLocalJob(**curIter);
	    } else {
		newState = batch_.checkStatus(**curIter);
	    }
	    (*curIter)->clearStatusErr();
	} catch (const runtime_error& err) {
	    log(LogError, err.what());
	    (*curIter)->addStatusErr();
	    if ((*curIter)->statusErrCnt() >= maxStatusErr_) {
		log(LogError, "too many errors for job " + (*curIter)->name());
		(*curIter)->changeStatus(Job::Failed);
	    }
	}
	if (newState) {
	    ostringstream msg;
	    msg << "job " << (*curIter)->name() << ' '
		<< (*curIter)->statusString();
	    if ((*curIter)->hasException()) {
		msg << " EXCEPTION";
	    }
	    log(LogWarning, msg.str());
	    changed = true;
	}
	curIter = nextIter;
    }
    return changed;
}

void
JobMonitor::initFiles (const string& jobFile, const string& outFile,
		       const string& logFile)
{
    if (!outFile.empty()) {
	statusFile_ = outFile;
    } else {
	ostringstream statusFile;
	smatch matches;
	if (regex_match(jobFile, matches, filenameRE)) {
	    string basename(matches[1].first, matches[1].second);
	    string number(matches[2].first, matches[2].second);
	    if (number.empty()) {
		statusFile << basename << ".status1";
	    } else {
		unsigned lastNumber = strtoul(number.c_str(), 0, 10);
		statusFile << basename << ".status" << lastNumber + 1;
	    }
	} else {
	    statusFile << jobFile << ".status";
	}
	statusFile_ = statusFile.str();
    }
    backupFile_ = statusFile_ + ".bak";
    ofstream ofs(statusFile_.c_str());
    if (!ofs) {
	throw runtime_error("cannot open status file " + statusFile_);
    }
    ofs.close();
    cout << "Status file: " << statusFile_ << endl;

    logFile_.assign(logFile);
    if (logFile_.empty() && !interactive_) {
	logFile_ = statusFile_ + ".log";
    }
    if (!logFile_.empty()) {
	logOfs_.open(logFile_.c_str());
	if (!logOfs_) {
	    throw runtime_error("cannot open log file " + logFile_);
	}
	cout << "Log file: " << logFile_ << endl;
	logOs_ = &logOfs_;
    } else {
	logOs_ = &cout;
    }
}

void
JobMonitor::saveStatus (bool backup)
{
    if (backup) {
	if (rename(statusFile_.c_str(), backupFile_.c_str()) < 0) {
	    ostringstream err;
	    err << "could not back up log file: " << strerror(errno);
	    log(LogError, err.str());
	}
    }
    ofstream ofs(statusFile_.c_str());
    if (!ofs) {
	throw("cannot open status file " + statusFile_);
    }
    jobs_.print(ofs);
    ofs.close();
}

void
JobMonitor::logStatus ()
{
    ostringstream msg;
    collectStatus(msg);
    log(LogAlways, msg.str());
}

void
JobMonitor::collectStatus (ostringstream& msg)
{
    bool first = true;
    printJobCount(msg, jobs_.waiting(), Job::Waiting, first);
    printJobCount(msg, jobs_.ready(), Job::Ready, first);
    printJobCount(msg, jobs_.dispatched(), Job::Dispatched, first);
    printJobCount(msg, jobs_.pending(), Job::Pending, first);
    printJobCount(msg, jobs_.running(), Job::Running, first);
    printJobCount(msg, jobs_.done(), Job::Done, first);
    printJobCount(msg, jobs_.failed(), Job::Failed, first);
    unsigned exceptionCount = 0;
    for (JobGraph::JobMap::const_iterator iter = jobs_.begin();
	 iter != jobs_.end(); ++iter) {
	if (iter->second->hasException()) {
	    ++exceptionCount;
	}
    }
    if (exceptionCount > 0) {
	if (!first) {
	    msg << " ";
	}
	msg << "[" << exceptionCount << " exceptions]";
    }
}

void
JobMonitor::printJobCount (ostream& os, const JobGraph::JobList& list,
			   Job::Status status, bool& first)
{
    if (list.size() > 0) {
	if (first) {
	    first = false;
	} else {
	    os << ", ";
	}
	os << list.size() << " " << Job::statusString(status);
    }
}

void
JobMonitor::logResults ()
{
    unsigned finished = true;
    if (jobs_.done().size() > 0) {
	*logOs_ << "\nSuccessful jobs:\n";
	logJobs(jobs_.done(), true);
    }
    if (jobs_.failed().size() > 0) {
	*logOs_ << "\nFailed jobs:\n";
	logJobs(jobs_.failed(), true);
	finished = false;
    }
    if (jobs_.waiting().size() > 0 ||
	jobs_.ready().size() > 0 ||
	jobs_.dispatched().size() > 0 ||
	jobs_.pending().size() > 0 ||
	jobs_.running().size() > 0) {
	*logOs_ << "\nIncomplete jobs:\n";
	logJobs(jobs_.waiting(), false);
	logJobs(jobs_.ready(), false);
	logJobs(jobs_.dispatched(), false);
	logJobs(jobs_.pending(), false);
	logJobs(jobs_.running(), false);
	finished = false;
    }
    *logOs_ << "\nFinal summary:\n";
    logStatus();
    if (!finished) {
	*logOs_ << "\nTo run the unfinished jobs, make a copy of "
		<< statusFile_ << " and rerun sjm on it.\n\n";
    }

    unsigned long long wallclockTime = 0;
    unsigned long long cpuTime = 0;
    unsigned maxMemory = 0;
    unsigned maxSwap = 0;
    for (JobGraph::JobMap::iterator jobIter = jobs_.begin();
	 jobIter != jobs_.end(); ++jobIter) {
	Job& job = *jobIter->second;
	wallclockTime += job.wallclockTime();
	cpuTime += job.cpuTime();
	if (job.memoryUsage() > maxMemory) {
	    maxMemory = job.memoryUsage();
	}
	if (job.swapUsage() > maxSwap) {
	    maxSwap = job.swapUsage();
	}
    }
    unsigned hours = wallclockTime / 3600;
    unsigned minutes = (wallclockTime % 3600) / 60;
    unsigned seconds = wallclockTime % 60;
    *logOs_ << "Total wallclock time: "
	    << setfill('0') << hours << ':' << setw(2) << minutes << ':'
	    << setw(2) << seconds << setfill(' ') << "\n";
    *logOs_ << "CPU time: " << cpuTime << " sec\n";
    *logOs_ << "Maximum memory: " << maxMemory << " MB\n";
    *logOs_ << "Maximum swap: " << maxSwap << " MB\n" << flush;
}

void
JobMonitor::logJobs (JobGraph::JobList& jobList, bool logStats)
{
    for (JobGraph::JobList::iterator iter = jobList.begin();
	 iter != jobList.end(); ++iter) {
	*logOs_ << (*iter)->name() << " (" << (*iter)->jobId() << "):";
	if (logStats) {
	    unsigned hours = (*iter)->wallclockTime() / 3600;
	    unsigned minutes = ((*iter)->wallclockTime() % 3600) / 60;
	    unsigned seconds = (*iter)->wallclockTime() % 60;
	    *logOs_ << ' ' << setfill('0')
		    << hours << ':' << setw(2) << minutes << ':'
		    << setw(2) << seconds << setfill(' ')
		    << " (" << (*iter)->cpuTime() << " sec"
		    << ", " << (*iter)->memoryUsage()
		    << '/' << (*iter)->swapUsage() << " MB)";
	    if ((*iter)->exitCode() != 0) {
		*logOs_ << " exit=" << (*iter)->exitCode();
	    }
	} else {
	    *logOs_ << ' ' << (*iter)->statusString();
	}
	*logOs_ << '\n';
    }
    *logOs_ << flush;
}

bool
JobMonitor::checkSignals ()
{
    if (GotQuit) {
	GotQuit = false;
	if (interactive_) {
	    cerr << "Quit" << endl;
	}
	log(LogError, "received QUIT signal");
	kill();
	return true;
    }
    if (GotInterrupt) {
	GotInterrupt = false;
	if (interactive_) {
	    cerr << "Interrupt" << endl;
	    cerr << "Kill running jobs? [y/n] " << flush;
	    string line;
	    getline(cin, line);
	    if (line.size() == 0 || (line[0] != 'y' && line[0] != 'Y')) {
		cerr << "Continuing..." << endl;
		return false;
	    }
	}
	log(LogError, "received INTERRUPT signal");
	kill();
	return true;
    }
    return false;
}

void
JobMonitor::kill ()
{
    for (JobGraph::JobMap::const_iterator iter = jobs_.begin();
	 iter != jobs_.end(); ++iter) {
	Job& job = *(iter->second);
	if (job.status() == Job::Dispatched || 
	    job.status() == Job::Pending ||
	    job.status() == Job::Running) {
	    ostringstream msg;
	    msg << "killing " << job.name();
	    log(LogInfo, msg.str());
	    try {
		if (job.runLocally()) {
		    killLocalJob(job);
		} else {
		    batch_.kill(job);
		}
	    } catch (const runtime_error& err) {
		log(LogError, err.what());
	    }
	}
    }
}

// run a job on the local host
bool
JobMonitor::runLocalJob (Job& job)
{
    // open log files
    int outFd = -1, errFd = -1;
    if (!job.jobGraph().logDir().empty()) {
	string outFilename;
	job.stdoutLogFilename(outFilename);
	outFd = creat(outFilename.c_str(), 0666);
	if (outFd < 0) {
	    ostringstream err;
	    err << "cannot open output log file " << outFilename << ": "
		<< strerror(errno);
	    throw runtime_error(err.str());
	}

	string errFilename;
	job.stderrLogFilename(errFilename);
	errFd = creat(errFilename.c_str(), 0666);
	if (errFd < 0) {
	    ostringstream err;
	    err << "cannot open error log file " << errFilename << ": "
		<< strerror(errno);
	    throw runtime_error(err.str());
	}
    }

    // create child process
    int pid = fork();
    if (pid < 0) {
	ostringstream err;
	err << "cannot run job " << job.name() << ": fork failed";
	throw runtime_error(err.str());
    }
    if (pid == 0) {
	// child process only
	close(0);
	if (outFd >= 0) {
	    dup2(outFd, 1);
	    close(outFd);
	}
	if (errFd >= 0) {
	    dup2(errFd, 2);
	    close(errFd);
	}
	execl("/bin/sh", "sh", "-c", job.augmentedCommand().c_str(),
	      (char *)0);
	_exit(1);
    }
    job.setJobId(pid);
    job.changeStatus(Job::Running);
    return true;
}

// check the status of a job on the local host
bool
JobMonitor::checkLocalJob (Job& job)
{
    int status = 0;
    struct rusage res;
    int pid = wait4(job.jobId(), &status, WNOHANG, &res);
    if (pid < 0) {
	ostringstream err;
	err << "cannot check job " << job.name() << ": wait4 failed: "
	    << pid;
	throw runtime_error(err.str());
    }
    if (pid == 0) {
	return false;
    }
    if (WIFEXITED(status)) {
	unsigned exitCode = WEXITSTATUS(status);
	if (exitCode == 0) {
	    job.changeStatus(Job::Done);
	} else {
	    job.changeStatus(Job::Failed);
	    job.setExitCode(exitCode);
	}
    } else {
	job.changeStatus(Job::Failed);
    }
    job.setCpuTime(res.ru_utime.tv_sec + res.ru_stime.tv_sec +
		   (res.ru_utime.tv_usec + res.ru_stime.tv_usec) / 1000000);
    job.setMemoryUsage(res.ru_maxrss * 1024);
    return true;
}

// kill a job on the local host
void
JobMonitor::killLocalJob (Job& job)
{
    ::kill(job.jobId(), SIGKILL);
    job.changeStatus(Job::Failed);
}

// return current time in seconds
unsigned
JobMonitor::currentTime ()
{
    struct timeval tv;
    if (gettimeofday(&tv, 0) < 0) {
	ostringstream err;
	err << "gettimeofday failed: " << strerror(errno);
	throw runtime_error(err.str());
    }
    return tv.tv_sec;
}

bool
JobMonitor::render (const string& jobFile, const string& outFile)
{
    try {
	jobs_.load(jobFile, true);
    } catch (const runtime_error& err) {
	cerr << "sjm: " << err.what() << endl;
	return false;
    }
    if (outFile.empty()) {
	jobs_.render(cout);
    } else {
	ofstream ofs(outFile.c_str());
	if (!ofs) {
	    cerr << "sjm: cannot open output file " << outFile << endl;
	    return false;
	}
	jobs_.render(ofs);
	ofs.close();
    }
    return true;
}

void
JobMonitor::setDispatchCount (unsigned dispatchCount)
{
    if (dispatchCount == 0) {
	dispatchCount_ = defDispatchCount;
    } else {
	dispatchCount_ = dispatchCount;
    }
}

void
JobMonitor::setDispatchInterval (unsigned dispatchInterval)
{
    if (dispatchInterval == 0) {
	dispatchInterval_ = defDispatchInterval;
    } else {
	dispatchInterval_ = dispatchInterval;
    }
}

void
JobMonitor::setPendingJobLimit (unsigned pendingJobLimit)
{
    if (pendingJobLimit == 0) {
	pendingJobLimit_ = defPendingJobLimit;
    } else {
	pendingJobLimit_ = pendingJobLimit;
    }
}

void
JobMonitor::setRunningJobLimit (unsigned runningJobLimit)
{
    runningJobLimit_ = runningJobLimit;
}

void
JobMonitor::setCheckJobInterval (unsigned checkJobInterval)
{
    if (checkJobInterval == 0) {
	checkJobInterval_ = defCheckJobInterval;
    } else {
	checkJobInterval_ = checkJobInterval;
    }
}

void
JobMonitor::setSaveInterval (unsigned saveInterval)
{
    if (saveInterval == 0) {
	saveInterval_ = defSaveInterval;
    } else {
	saveInterval_ = saveInterval;
    }
}

void
JobMonitor::setLogInterval (unsigned logInterval)
{
    if (logInterval == 0) {
	logInterval_ = defLogInterval;
    } else {
	logInterval_ = logInterval;
    }
}

void
JobMonitor::setMaxStatusErr (unsigned maxStatusErr)
{
    if (maxStatusErr == 0) {
	maxStatusErr_ = defMaxStatusErr;
    } else {
	maxStatusErr_ = maxStatusErr;
    }
}

void
JobMonitor::setLogLevel (LogLevel level)
{
    logLevel_ = level;
}

void
JobMonitor::log (LogLevel level, const string& msg)
{
    if (level < logLevel_) {
	return;
    }
    time_t clock = currentTime();
    string curTime(ctime(&clock));
    curTime.erase(curTime.size() - 1);
    *logOs_ << curTime << ": " << msg << endl << flush;
}

void
JobMonitor::sendEmail (const vector<string>& mailAddr, const string& subject,
		       const string& message)
{
    ostringstream cmd;
    cmd << "mailx -s \"" << subject << "\"";
    for (unsigned idx = 0; idx < mailAddr.size(); ++idx) {
	cmd << " " << mailAddr[idx];
    }
    FILE *fp;
    if ((fp = popen(cmd.str().c_str(), "w")) == NULL) {
	log(LogError, "cannot send email: popen failed");
	return;
    }
    fputs(message.c_str(), fp);
    fputs("\n", fp);
    if (pclose(fp) < 0) {
	log(LogError, "cannot send email: pclose failed");
	return;
    }
}

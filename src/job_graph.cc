//
// job_graph.cc - sjm job graph
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

static char const rcsid[] = "$Id: job_graph.cc 240 2009-03-19 21:51:09Z lacroute $";

#include <iostream>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <memory>
#include <cstdlib>
#include <vector>

#include "../config.h"
#include "job_graph.hh"

using std::ostream;
using std::ifstream;
using std::ostringstream;
using std::endl;
using std::runtime_error;
using std::logic_error;
using std::auto_ptr;
using std::string;
using std::map;
using std::list;
using std::vector;
using std::pair;
using std::make_pair;

using boost::regex;
using boost::smatch;
using boost::regex_match;

const string Job::unknownString("unknown");
const string Job::waitingString("waiting");
const string Job::readyString("ready");
const string Job::dispatchedString("dispatched");
const string Job::pendingString("pending");
const string Job::runningString("running");
const string Job::doneString("done");
const string Job::failedString("failed");
const string Job::invalidString("invalid");

Job::Job ():
    memoryLimit_(0),
    timeLimit_(0),
    runLocally_(false),
    slots_(0),
    jobId_(0),
    status_(Unknown),
    cpuTime_(0),
    wallclockTime_(0),
    memoryUsage_(0),
    swapUsage_(0),
    exitCode_(0),
    jobGraph_(0),
    statusErrCnt_(0),
    exception_(false),
    visited_(false)
{
}

const string&
Job::statusString () const
{
    return statusString(status_);
}

const string&
Job::statusString (Status status)
{
    switch (status) {
    case Unknown:
	return unknownString;
    case Waiting:
	return waitingString;
    case Ready:
	return readyString;
    case Dispatched:
	return dispatchedString;
    case Pending:
	return pendingString;
    case Running:
	return runningString;
    case Done:
	return doneString;
    case Failed:
	return failedString;
    default:
	return invalidString;
    }
}

// change the status of a job
void
Job::changeStatus (Status status)
{
    if (status <= status_) {
	ostringstream err;
	err << "illegal status change for " << name_ << " from "
	    << statusString() << " to " << statusString(status);
	throw logic_error(err.str());
    }
    jobGraph_->changeJobStatus(this, status);
}

bool
Job::ready () const
{
    for (JobList::const_iterator iter = before_.begin(); iter != before_.end();
	 ++iter) {
	if ((*iter)->status() != Done) {
	    return false;
	}
    }
    return true;
}

const string
Job::augmentedCommand ()
{
    if (modules().empty()) {
	return command();
    }
    char *run_with_env = getenv("RUN_WITH_ENV");
    if (run_with_env != 0) {
	augmentedCommand_.assign(run_with_env);
    } else {
	augmentedCommand_.assign(BINDIR);
	augmentedCommand_ += "/run_with_env";
    }
    augmentedCommand_ += " --module ";
    bool first = true;
    for (Job::ModuleList::const_iterator iter = modules().begin();
	 iter != modules().end(); ++iter) {
	if (first) {
	    first = false;
	} else {
	    augmentedCommand_ += ",";
	}
	augmentedCommand_ += *iter;
    }
    augmentedCommand_ += " -- " + command();
    return augmentedCommand_;
}

// construct the stdout log filename
void
Job::stdoutLogFilename (std::string& filename)
{
    filename.assign(jobGraph().logDir() + "/" +
		    name() + "_o" + jobGraph().runId() + ".txt");
}

// construct the stderr log filename
void
Job::stderrLogFilename (std::string& filename)
{
    filename.assign(jobGraph().logDir() + "/" +
		    name() + "_e" + jobGraph().runId() + ".txt");
}

JobGraph::JobGraph ()
{
    jobList_.resize(Job::NumStatus);
}

// load the job graph from a file
void
JobGraph::load (const string& jobFile, bool preserveState)
{
    JobGraphParser parser;
    parser.parse(jobFile, *this, preserveState);
}

// add a job to the job graph
void
JobGraph::add (Job* job)
{
    if (find(job->name()) != 0) {
	logic_error("duplicate job " + job->name());
    }
    jobs_[job->name()] = job;
    jobList_[job->status()].push_back(job);
    job->jobGraph_ = this;
    job->listPos_ = --(jobList_[job->status()].end());
}

// add a dependency to the job graph
void
JobGraph::order (Job* firstJob, Job* secondJob)
{
    firstJob->after_.push_back(secondJob);
    secondJob->before_.push_back(firstJob);
}

// set the log directory
void
JobGraph::setLogDir (const std::string& logDir)
{
    logDir_.assign(logDir);
}

// set the run ID
void
JobGraph::setRunId (const std::string& runId)
{
    runId_.assign(runId);
}

// get the log directory
const std::string&
JobGraph::logDir () const
{
    return logDir_;
}

// get the run ID
const std::string&
JobGraph::runId () const
{
    return runId_;
}

// check for cycles
void
JobGraph::validate ()
{
    // clear the visited flag on all nodes
    for (JobMap::iterator iter = jobs_.begin(); iter != jobs_.end(); ++iter) {
	iter->second->visited_ = false;
    }

    // make sure there is a topological ordering of the nodes
    unsigned visitedCnt = 0;
    while (visitedCnt < jobs_.size()) {
	// look for all jobs that have no unvisited jobs that must run first,
	// and mark each such job as visited
	unsigned foundCnt = 0;
	for (JobMap::iterator iter = jobs_.begin(); iter != jobs_.end();
	     ++iter) {
	    Job& job = *(iter->second);
	    if (job.visited_) {
		continue;
	    }
	    bool ready = true;
	    for (JobList::iterator before = job.before_.begin();
		 before != job.before_.end(); ++before) {
		if (!(*before)->visited_) {
		    ready = false;
		    break;
		}
	    }
	    if (ready) {
		job.visited_ = true;
		++foundCnt;
	    }
	}
	if (foundCnt == 0) {
	    Job* cycleJob = 0;
	    for (JobMap::iterator iter = jobs_.begin(); iter != jobs_.end();
		 ++iter) {
		if (!iter->second->visited_) {
		    cycleJob = iter->second;
		    break;
		}
	    }
	    if (cycleJob != 0) {
		throw runtime_error("job graph has a cycle including job " +
				    cycleJob->name());
	    } else {
		throw logic_error("cycle detection failed");
	    }
	}
	visitedCnt += foundCnt;
    }

    // clear the visited_ flag
    for (JobMap::iterator iter = jobs_.begin(); iter != jobs_.end(); ++iter) {
	iter->second->visited_ = false;
    }
}

void
JobGraph::changeJobStatus (Job* job, Job::Status status)
{
    jobList_[job->status_].erase(job->listPos_);
    jobList_[status].push_back(job);
    job->listPos_ = --(jobList_[status].end());
    job->status_ = status;
    if (status == Job::Dispatched) {
	if (find(job->jobId()) != 0) {
	    logic_error("duplicate job ID " + job->jobId());
	}
	jobsById_[job->jobId()] = job;
    } else if (status == Job::Done) {
	// move dependent jobs to ready list
	for (JobList::iterator iter = job->after_.begin();
	     iter != job->after_.end(); ++iter) {
	    if ((*iter)->ready()) {
		changeJobStatus(*iter, Job::Ready);
	    }
	}
    }
}

// find a job by its name
Job*
JobGraph::find (const string& jobname)
{
    JobMap::iterator iter = jobs_.find(jobname);
    if (iter == jobs_.end()) {
	return 0;
    } else {
	return iter->second;
    }
}

// find a job by its jobId
Job*
JobGraph::find (Job::JobId jobId)
{
    JobIdMap::iterator iter = jobsById_.find(jobId);
    if (iter == jobsById_.end()) {
	return 0;
    } else {
	return iter->second;
    }
}

// return the first ready job
Job*
JobGraph::firstReady () const
{
    if (jobList_[Job::Ready].empty()) {
	return 0;
    } else {
        return jobList_[Job::Ready].front();
    }
}

bool
JobGraph::finished () const
{
    if (jobList_[Job::Ready].empty() &&
	jobList_[Job::Dispatched].empty() &&
	jobList_[Job::Pending].empty() &&
	jobList_[Job::Running].empty()) {
	return true;
    }
    return false;
}

bool
JobGraph::succeeded () const
{
    if (jobList_[Job::Done].size() == jobs_.size()) {
	return true;
    }
    return false;
}

// print the job graph in the same format as the input file
void
JobGraph::print (ostream& os) const
{
    if (!logDir_.empty()) {
	os << "log_dir " << logDir_ << '\n';
    }
    for (JobMap::const_iterator iter = jobs_.begin();
	 iter != jobs_.end(); ++iter) {
	Job& job = *(iter->second);
	os << "job_begin\n";
	os << "    name " << job.name() << '\n';
	if (job.memoryLimit() > 0) {
	    os << "    memory " << job.memoryLimit() << " M\n";
	}
	if (job.timeLimit() > 0) {
	    os << "    time " << job.timeLimit() << " h\n";
	}
	if (!job.queue().empty()) {
	    os << "    queue " << job.queue() << "\n";
	}
	if (!job.project().empty()) {
	    os << "    project " << job.project() << "\n";
	}
	if (!job.schedOptions().empty()) {
	    os << "    sched_options " << job.schedOptions() << "\n";
	}
	if (job.runLocally()) {
	    os << "    host localhost\n";
	}
	for (Job::EnvVarList::const_iterator iter = job.exportVars().begin();
	     iter != job.exportVars().end(); ++iter) {
	    os << "    export " << iter->first;
	    if (!iter->second.empty()) {
		os << "=" << iter->second;
	    }
	    os << "\n";
	}
	for (Job::ModuleList::const_iterator iter = job.modules().begin();
	     iter != job.modules().end(); ++iter) {
	    os << "    module " << *iter << "\n";
	}
	if (!job.directory().empty()) {
	    os << "    directory " << job.directory() << "\n";
	}
	if (job.slots() != 0) {
	    os << "    slots " << job.slots() << "\n";
	}
	if (!job.parallelEnv().empty()) {
	    os << "    parallel_env " << job.parallelEnv() << "\n";
	}
	os << "    status " << job.statusString() << '\n';
	if (job.status() == Job::Dispatched ||
	    job.status() == Job::Pending ||
	    job.status() == Job::Running ||
	    job.status() == Job::Done ||
	    job.status() == Job::Failed) {
	    os << "    id " << job.jobId() << '\n';
	}
	if (job.status() == Job::Running ||
	    job.status() == Job::Done ||
	    job.status() == Job::Failed) {
	    os << "    cpu_usage " << job.cpuTime() << '\n';
	    os << "    wallclock " << job.wallclockTime() << '\n';
	    os << "    memory_usage " << job.memoryUsage() << '\n';
	    os << "    swap_usage " << job.swapUsage() << '\n';
	}
	if (job.command().find('\n') == string::npos) {
	    os << "    cmd " << job.command() << '\n';
	} else {
	    os << "    cmd_begin\n";
	    os << job.command();
	    os << "    cmd_end\n";
	}
	os << "job_end\n";
    }

    for (JobMap::const_iterator iter = jobs_.begin();
	 iter != jobs_.end(); ++iter) {
	Job& job = *(iter->second);
	for (JobList::const_iterator depIter = job.before_.begin();
	     depIter != job.before_.end(); ++depIter) {
	    os << "order " << job.name() << " after " << (*depIter)->name()
	       << '\n';
	}
    }
}

// render the job graph in dot format
void
JobGraph::render (ostream& os) const
{
    os << "digraph jobs {\n";
    for (JobMap::const_iterator iter = jobs_.begin();
	 iter != jobs_.end(); ++iter) {
	Job& job = *(iter->second);
	os << "    " << job.name() << "[color=";
	switch (job.status()) {
	case Job::Unknown:
	    os << "yellow";
	    break;
	case Job::Waiting:
	    os << "gray";
	    break;
	case Job::Ready:
	    os << "black";
	    break;
	case Job::Dispatched:
	    os << "lightblue";
	    break;
	case Job::Pending:
	    os << "purple";
	    break;
	case Job::Running:
	    os << "blue";
	    break;
	case Job::Done:
	    os << "green";
	    break;
	case Job::Failed:
	    os << "red";
	    break;
	default:
	    os << "orange";
	    break;
	}
	os << "];\n";
	for (JobList::const_iterator after = job.after_.begin();
	     after != job.after_.end(); ++after) {
	    os << "    " << job.name() << " -> " << (*after)->name() << ";\n";
	}
    }
    os << "}" << endl;
}

void
JobGraph::sanity () const
{
    if (jobList_[Job::Unknown].size() != 0) {
	ostringstream err;
	err << jobList_[Job::Unknown].size() << " jobs on Unknown list";
	throw logic_error(err.str());
    }

    // check consistency of job lists
    unsigned jobCnt = 0;
    for (unsigned status = Job::Waiting; status != Job::NumStatus;
	 ++status) {
	jobCnt += jobList_[status].size();
	for (JobList::const_iterator iter = jobList_[status].begin();
	     iter != jobList_[status].end(); ++iter) {
	    if ((unsigned)((*iter)->status()) != status) {
		ostringstream err;
		err << "job " << (*iter)->name() << " has status "
		    << (*iter)->statusString() << " but is on list "
		    << Job::statusString((Job::Status)status);
		throw logic_error(err.str());
	    }
	    if (*((*iter)->listPos_) != *iter) {
		ostringstream err;
		err << "job " << (*iter)->name()
		    << " list pointer points to wrong object";
		throw logic_error(err.str());
	    }
	    JobList::const_iterator jobNext = (*iter)->listPos_;
	    JobList::const_iterator listNext = iter;
	    ++jobNext;
	    ++listNext;
	    if (*jobNext != *listNext) {
		ostringstream err;
		err << "job " << (*iter)->name()
		    << " list pointer points to wrong list";
		throw logic_error(err.str());
	    }
	    if (status == Job::Waiting) {
		if ((*iter)->ready()) {
		    ostringstream err;
		    err << "job " << (*iter)->name()
			<< " is ready but is on Waiting list";
		    throw logic_error(err.str());
		}
	    } else {
		if (!(*iter)->ready()) {
		    ostringstream err;
		    err << "job " << (*iter)->name()
			<< " with unsatisfied dependency has status "
			<< (*iter)->statusString();
		    throw runtime_error(err.str());
		}
	    }
	}
    }

    if (jobCnt != jobs_.size()) {
	ostringstream err;
	err << "Found " << jobCnt
	    << " jobs on job lists but total job count is "
	    << jobs_.size();
	throw logic_error(err.str());
    }

    for (JobMap::const_iterator iter = jobs_.begin();
	 iter != jobs_.end(); ++iter) {
	const Job& job = *iter->second;
	if (job.jobGraph_ != this) {
	    ostringstream err;
	    err << "Job " << iter->second->name() << " has bad jobGraph link";
	    throw logic_error(err.str());
	}
	if (iter->first != job.name()) {
	    ostringstream err;
	    err << "Job " << iter->second->name() << " indexed as "
		<< iter->first;
	    throw logic_error(err.str());
	}
    }
}

const regex JobGraphParser::commentRE("\\s*#.*");
const regex JobGraphParser::blankRE("\\s*");
const regex JobGraphParser::jobBeginRE("\\s*job_begin\\s*");
const regex JobGraphParser::jobEndRE("\\s*job_end\\s*");
const regex JobGraphParser::nameRE("\\s*name\\s+(\\S+)\\s*");
const regex JobGraphParser::memoryRE("\\s*memory\\s+(\\d+)"
				     "\\s*([bkmgBKMG]?)\\s*");
const regex JobGraphParser::timeRE("\\s*time\\s+(\\d+)\\s*([hmsdHMSD]?)\\s*");
const regex JobGraphParser::queueRE("\\s*queue\\s+(\\S+)\\s*");
const regex JobGraphParser::projectRE("\\s*project\\s+(\\S+)\\s*");
const regex JobGraphParser::sgeOptionsRE("\\s*sge_options\\s+(.+)");
const regex JobGraphParser::schedOptionsRE("\\s*sched_options\\s+(.+)");
const regex JobGraphParser::cmdRE("\\s*cmd\\s+(\\S.*)");
const regex JobGraphParser::cmdBeginRE("\\s*cmd_begin\\s*");
const regex JobGraphParser::cmdEndRE("\\s*cmd_end\\s*");
const regex JobGraphParser::hostRE("\\s*host\\s+(\\S+)\\s*");
const regex JobGraphParser::statusRE("\\s*status\\s+(waiting|ready|"
				     "dispatched|pending|running|done|"
				     "failed)\\s*");
const regex JobGraphParser::orderRE("\\s*order\\s+(\\S+)\\s+(before|after)"
				    "\\s+(\\S+)\\s*");
const regex JobGraphParser::idRE("\\s*id\\s+(\\d+)\\s*");
const regex JobGraphParser::cpuUsageRE("\\s*cpu_usage\\s+(\\d+)\\s*");
const regex JobGraphParser::wallclockRE("\\s*wallclock\\s+(\\d+)\\s*");
const regex JobGraphParser::memoryUsageRE("\\s*memory_usage\\s+(\\d+)\\s*");
const regex JobGraphParser::swapUsageRE("\\s*swap_usage\\s+(\\d+)\\s*");
const regex JobGraphParser::logDirRE("\\s*log_dir\\s+(\\S+)\\s*");
const regex JobGraphParser::exportRE("\\s*export\\s+([^\\s=]+)(=(\\S+))?\\s*");
const regex JobGraphParser::moduleRE("\\s*module\\s+(\\S+)\\s*");
const regex JobGraphParser::directoryRE("\\s*directory\\s+(\\S+)\\s*");
const regex JobGraphParser::slotsRE("\\s*slots\\s+(\\d+)\\s*");
const regex JobGraphParser::parallelEnvRE("\\s*parallel_env\\s+(\\S+)\\s*");

JobGraphParser::JobGraphParser ():
    parseMode_(Start),
    lineNum_(0)
{
}

void
JobGraphParser::parse (const string& jobFile, JobGraph& jobs,
		       bool preserveState)
{
    jobFile_ = jobFile;
    jobs_ = &jobs;
    preserveState_ = preserveState;

    ifstream ifs(jobFile_.c_str());
    if (!ifs) {
	ostringstream err;
	err << "cannot open job file " << jobFile_;
	throw runtime_error(err.str());
    }

    parseMode_ = Start;
    lineNum_ = 0;
    curJob_.reset();
    string line;
    while (getline(ifs, line)) {
	++lineNum_;
	switch (parseMode_) {
	case Start:
	    parseGlobal(line);
	    break;
	case FoundJobBegin:
	    parseJob(line);
	    break;
	case FoundCmdBegin:
	    parseCmd(line);
	    break;
	default:
	    throw logic_error("invalid parseMode");
	}
    }

    if (parseMode_ != Start) {
	ostringstream err;
	err << jobFile_ << ':' << lineNum_ << ": missing job_end";
	throw runtime_error(err.str());
    }
    ifs.close();

    // initialize ready list
    if (!preserveState_) {
	for (JobGraph::JobMap::iterator iter = jobs.begin();
	     iter != jobs.end(); ++iter) {
	    Job& job = *iter->second;
	    if (job.status() == Job::Waiting && job.ready()) {
		job.changeStatus(Job::Ready);
	    }
	}
    }
}

void
JobGraphParser::parseGlobal (const string& line)
{
    smatch matches;
    if (regex_match(line, commentRE) || regex_match(line, blankRE)) {
	return;
    } else if (regex_match(line, jobBeginRE)) {
	parseMode_ = FoundJobBegin;
	curJob_.reset(new Job);
	curJob_->status_ = Job::Waiting;
    } else if (regex_match(line, matches, orderRE)) {
	string name1(matches[1].first, matches[1].second);
	string orderString(matches[2].first, matches[2].second);
	string name2(matches[3].first, matches[3].second);
	Job* job1 = jobs_->find(name1);
	if (job1 == 0) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_
		<< ": invalid job name " << name1;
	    throw runtime_error(err.str());
	}
	Job* job2 = jobs_->find(name2);
	if (job2 == 0) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_
		<< ": invalid job name " << name2;
	    throw runtime_error(err.str());
	}
	if (orderString == "before") {
	    jobs_->order(job1, job2);
	} else if (orderString == "after") {
	    jobs_->order(job2, job1);
	} else {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_
		<< ": invalid order " << orderString;
	    throw logic_error(err.str());
	}
    } else if (regex_match(line, matches, logDirRE)) {
	string logDir(matches[1].first, matches[1].second);
	jobs_->setLogDir(logDir);
    } else {
	ostringstream err;
	err << jobFile_ << ':' << lineNum_
	    << ": invalid command or missing job_begin: " << line;
	throw runtime_error(err.str());
    }
}

void
JobGraphParser::parseJob (const string& line)
{
    smatch matches;
    if (regex_match(line, commentRE) || regex_match(line, blankRE)) {
	return;
    } else if (regex_match(line, jobEndRE)) {
	if (curJob_->name().empty()) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": missing job name";
	    throw runtime_error(err.str());
	}
	if (curJob_->command().empty()) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": missing job name";
	    throw runtime_error(err.str());
	}
	jobs_->add(curJob_.release());
	parseMode_ = Start;
    } else if (regex_match(line, matches, nameRE)) {
	curJob_->name_.assign(matches[1].first, matches[1].second);
	if (jobs_->find(curJob_->name_) != 0) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": duplicate job name "
		<< curJob_->name_;
	    throw runtime_error(err.str());
	}
    } else if (regex_match(line, matches, memoryRE)) {
	string memoryStr(matches[1].first, matches[1].second);
	unsigned memoryLimit = strtoul(memoryStr.c_str(), 0, 10);
	string units(matches[2].first, matches[2].second);
	if (units.empty() || units == "m" || units == "M") {
	    curJob_->memoryLimit_ = memoryLimit;
	} else if (units == "k" || units == "K") {
	    curJob_->memoryLimit_ = (memoryLimit + 999) / 1000;
	} else if (units == "b" || units == "B") {
	    curJob_->memoryLimit_ = (memoryLimit + 999999) / 1000000;
	} else if (units == "g" || units == "G") {
	    curJob_->memoryLimit_ = memoryLimit * 1000;
	} else {
	    // should not happen (memoryRE wouldn't have matched)
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": unexpected units "
		<< units;
	    throw logic_error(err.str());
	}
    } else if (regex_match(line, matches, timeRE)) {
	string timeStr(matches[1].first, matches[1].second);
	unsigned timeLimit = strtoul(timeStr.c_str(), 0, 10);
	string units(matches[2].first, matches[2].second);
	if (units.empty() || units == "h" || units == "H") {
	    curJob_->timeLimit_ = timeLimit;
	} else if (units == "m" || units == "M") {
	    curJob_->timeLimit_ = (timeLimit + 59) / 60;
	} else if (units == "s" || units == "S") {
	    curJob_->timeLimit_ = (timeLimit + 3599) / 3600;
	} else if (units == "d" || units == "D") {
	    curJob_->timeLimit_ = timeLimit * 24;
	} else {
	    // should not happen (timeRE wouldn't have matched)
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": unexpected units "
		<< units;
	    throw logic_error(err.str());
	}
    } else if (regex_match(line, matches, queueRE)) {
	curJob_->queue_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, matches, projectRE)) {
	curJob_->project_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, matches, schedOptionsRE)) {
	curJob_->schedOptions_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, matches, sgeOptionsRE)) {
	curJob_->schedOptions_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, matches, cmdRE)) {
	if (!curJob_->command_.empty()) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": duplicate command";
	    throw runtime_error(err.str());
	}
	curJob_->command_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, cmdBeginRE)) {
	if (!curJob_->command_.empty()) {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": duplicate command";
	    throw runtime_error(err.str());
	}
	parseMode_ = FoundCmdBegin;
    } else if (regex_match(line, matches, hostRE)) {
	string host(matches[1].first, matches[1].second);
	if (host == "localhost") {
	    curJob_->runLocally_ = true;
	} else {
	    ostringstream err;
	    err << jobFile_ << ':' << lineNum_ << ": invalid host " << host;
	    throw runtime_error(err.str());
	}
    } else if (regex_match(line, matches, exportRE)) {
	string env_var(matches[1].first, matches[1].second);
	string value(matches[3].first, matches[3].second);
	curJob_->exportVars_.push_back(make_pair(env_var, value));
#ifdef HAVE_MODULES
    } else if (regex_match(line, matches, moduleRE)) {
	string module(matches[1].first, matches[1].second);
	curJob_->modules_.push_back(module);
#endif
    } else if (regex_match(line, matches, directoryRE)) {
	curJob_->directory_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, matches, slotsRE)) {
	string slotsStr(matches[1].first, matches[1].second);
	curJob_->slots_ = strtoul(slotsStr.c_str(), 0, 10);
    } else if (regex_match(line, matches, parallelEnvRE)) {
	curJob_->parallelEnv_.assign(matches[1].first, matches[1].second);
    } else if (regex_match(line, matches, statusRE)) {
	string status(matches[1].first, matches[1].second);
	for (unsigned statusIdx = Job::Unknown; statusIdx < Job::NumStatus;
	     ++statusIdx) {
	    if (status == Job::statusString((Job::Status)statusIdx)) {
		if (statusIdx == Job::Done || preserveState_) {
		    curJob_->status_ = (Job::Status)statusIdx;
		}
	    }
	}
    } else if (regex_match(line, matches, idRE)) {
	string idStr(matches[1].first, matches[1].second);
	unsigned long long id = strtoull(idStr.c_str(), 0, 10);
	curJob_->setJobId(id);
    } else if (regex_match(line, matches, cpuUsageRE)) {
	string cpuUsageStr(matches[1].first, matches[1].second);
	unsigned cpuUsage = strtoul(cpuUsageStr.c_str(), 0, 10);
	curJob_->setCpuTime(cpuUsage);
    } else if (regex_match(line, matches, wallclockRE)) {
	string wallclockStr(matches[1].first, matches[1].second);
	unsigned wallclock = strtoul(wallclockStr.c_str(), 0, 10);
	curJob_->setWallclockTime(wallclock);
    } else if (regex_match(line, matches, memoryUsageRE)) {
	string memoryUsageStr(matches[1].first, matches[1].second);
	unsigned memoryUsage = strtoul(memoryUsageStr.c_str(), 0, 10);
	curJob_->setMemoryUsage(memoryUsage);
    } else if (regex_match(line, matches, swapUsageRE)) {
	string swapUsageStr(matches[1].first, matches[1].second);
	unsigned swapUsage = strtoul(swapUsageStr.c_str(), 0, 10);
	curJob_->setSwapUsage(swapUsage);
    } else if (regex_match(line, jobBeginRE)) {
	ostringstream err;
	err << jobFile_ << ':' << lineNum_ << ": unexpected job_begin";
	throw runtime_error(err.str());
    } else {
	ostringstream err;
	err << jobFile_ << ':' << lineNum_ << ": invalid command: " << line;
	throw runtime_error(err.str());
    }
}

void
JobGraphParser::parseCmd (const string& line)
{
    if (regex_match(line, cmdEndRE)) {
	parseMode_ = FoundJobBegin;
    } else {
	curJob_->command_.append(line);
	curJob_->command_.append("\n");
    }
}

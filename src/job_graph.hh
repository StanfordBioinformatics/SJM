//
// job_graph.hh - sjm job graph
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
// $Id: job_graph.hh 240 2009-03-19 21:51:09Z lacroute $
//

#ifndef JOB_GRAPH_H
#define JOB_GRAPH_H

#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <boost/regex.hpp>

class JobGraph;

class Job {
    friend class JobGraph;
    friend class JobGraphParser;

public:
    typedef unsigned long long JobId;
    typedef std::list<Job*> JobList;
    typedef std::pair<std::string,std::string> EnvVar;
    typedef std::vector<EnvVar> EnvVarList;
    typedef std::vector<std::string> ModuleList;

    typedef enum {
	Unknown = 0,
	Waiting,
	Ready,
	Dispatched,
	Pending,
	Running,
	Done,
	Failed,
	NumStatus
    } Status;

    Job();
    const std::string& name() const { return name_; }
    unsigned memoryLimit() const { return memoryLimit_; }
    unsigned timeLimit() const { return timeLimit_; }
    const std::string& queue() const { return queue_; }
    const std::string& project() const { return project_; }
    const std::string& schedOptions() const { return schedOptions_; }
    const std::string command() const { return command_; }
    const std::string augmentedCommand();
    const EnvVarList& exportVars() const { return exportVars_; }
    const ModuleList& modules() const { return modules_; }
    const std::string& directory() const { return directory_; }
    unsigned slots() const { return slots_; }
    const std::string& parallelEnv() const { return parallelEnv_; }
    bool runLocally() const { return runLocally_; }
    JobId jobId() const { return jobId_; }
    Status status() const { return status_; }
    const std::string& statusString() const;
    static const std::string& statusString(Status status);
    unsigned cpuTime() const { return cpuTime_; }
    unsigned wallclockTime() const { return wallclockTime_; }
    unsigned memoryUsage() const { return memoryUsage_; }
    unsigned swapUsage() const { return swapUsage_; }
    int exitCode() const { return exitCode_; }
    bool ready() const;
    void changeStatus(Status status);
    void setJobId(JobId jobId) { jobId_ = jobId; }
    void setCpuTime(unsigned seconds) { cpuTime_ = seconds; }
    void setWallclockTime(unsigned seconds) { wallclockTime_ = seconds; }
    void setMemoryUsage(unsigned megabytes) { memoryUsage_ = megabytes; }
    void setSwapUsage(unsigned megabytes) { swapUsage_ = megabytes; }
    void setExitCode(int exitCode) { exitCode_ = exitCode; }
    unsigned statusErrCnt() const { return statusErrCnt_; }
    void addStatusErr() { ++statusErrCnt_; }
    void clearStatusErr() { statusErrCnt_ = 0; }
    bool hasException() { return exception_; }
    void setException(bool exception) { exception_ = exception; }
    JobGraph& jobGraph() const { return *jobGraph_; }
    void stdoutLogFilename(std::string& filename);
    void stderrLogFilename(std::string& filename);

private:
    static const std::string unknownString;
    static const std::string waitingString;
    static const std::string readyString;
    static const std::string dispatchedString;
    static const std::string pendingString;
    static const std::string runningString;
    static const std::string doneString;
    static const std::string failedString;
    static const std::string invalidString;

    std::string name_;		// job name
    unsigned memoryLimit_;	// memory limit (Mbytes)
    unsigned timeLimit_;	// CPU time limit (hours)
    std::string queue_;		// queue
    std::string project_;	// project
    std::string schedOptions_;	// scheduler options
    std::string command_;	// command to run
    bool runLocally_;		// if true then run on local host
    EnvVarList exportVars_;	// list of environment variables
    ModuleList modules_;	// list of environment modules
    std::string directory_;	// directory for running the command
    unsigned slots_;		// number of job slots (CPU cores) required
    std::string parallelEnv_;	// parallel environment name
    std::string augmentedCommand_; // command with internally-generated options
    JobId jobId_;		// job ID from batch system
    Status status_;		// job status
    unsigned cpuTime_;		// CPU time used in seconds
    unsigned wallclockTime_;	// wallclock time used in seconds
    unsigned memoryUsage_;	// memory used (Mbytes)
    unsigned swapUsage_;	// swap used (Mbytes)
    int exitCode_;		// process exit code
    JobGraph* jobGraph_;	// job graph containing this job
    JobList::iterator listPos_; // iterator pointing back to this object
				// through the list associated status_
				// (so this object can be removed from
				// the list)
    JobList before_;		// jobs that must run before this one
    JobList after_;		// jobs that must run after this one
    unsigned statusErrCnt_;	// error counter for status queries
    bool exception_;		// job has an active exceptional condition
    bool visited_;
};

class JobGraph {
public:
    typedef Job::JobList JobList;
    typedef std::map<std::string,Job*> JobMap;
    typedef std::map<Job::JobId,Job*> JobIdMap;

    JobGraph();
    void load(const std::string& jobFile, bool preserveState = false);
    void add(Job* job);
    void order(Job* firstJob, Job* secondJob);
    void setLogDir(const std::string& logDir);
    void setRunId(const std::string& runId);
    const std::string& logDir() const;
    const std::string& runId() const;
    void validate();
    void changeJobStatus(Job* job, Job::Status status);
    Job* find(const std::string& jobname);
    Job* find(Job::JobId jobId);
    Job* firstReady() const;
    bool finished() const;
    bool succeeded() const;
    JobList& waiting() { return jobList_[Job::Waiting]; }
    JobList& ready() { return jobList_[Job::Ready]; }
    JobList& dispatched() { return jobList_[Job::Dispatched]; }
    JobList& pending() { return jobList_[Job::Pending]; }
    JobList& running() { return jobList_[Job::Running]; }
    JobList& done() { return jobList_[Job::Done]; }
    JobList& failed() { return jobList_[Job::Failed]; }
    JobMap::iterator begin() { return jobs_.begin(); }
    JobMap::iterator end() { return jobs_.end(); }
    void print(std::ostream& os) const;
    void render(std::ostream& os) const;
    void sanity() const;

private:
    JobMap jobs_;		// index of jobs by name
    JobIdMap jobsById_;		// index of jobs by queuing system id
    std::vector<JobList> jobList_; // a list of jobs for each Status type
    std::string logDir_;	// directory for log files
    std::string runId_;	        // run ID for log file names (e.g. timestamp)
};

class JobGraphParser {
public:
    JobGraphParser();
    void parse(const std::string& jobFile, JobGraph& jobs, bool preserveState);

private:
    static const boost::regex commentRE;
    static const boost::regex blankRE;
    static const boost::regex jobBeginRE;
    static const boost::regex jobEndRE;
    static const boost::regex nameRE;
    static const boost::regex memoryRE;
    static const boost::regex timeRE;
    static const boost::regex queueRE;
    static const boost::regex projectRE;
    static const boost::regex schedOptionsRE;
    static const boost::regex sgeOptionsRE;
    static const boost::regex cmdRE;
    static const boost::regex cmdBeginRE;
    static const boost::regex cmdEndRE;
    static const boost::regex hostRE;
    static const boost::regex statusRE;
    static const boost::regex orderRE;
    static const boost::regex idRE;
    static const boost::regex cpuUsageRE;
    static const boost::regex wallclockRE;
    static const boost::regex memoryUsageRE;
    static const boost::regex swapUsageRE;
    static const boost::regex logDirRE;
    static const boost::regex exportRE;
    static const boost::regex moduleRE;
    static const boost::regex directoryRE;
    static const boost::regex slotsRE;
    static const boost::regex parallelEnvRE;

    typedef enum {
	Start,
	FoundJobBegin,
	FoundCmdBegin
    } ParseMode;

    std::string jobFile_;
    ParseMode parseMode_;
    unsigned lineNum_;
    std::auto_ptr<Job> curJob_;
    JobGraph* jobs_;
    bool preserveState_;

    void parseGlobal(const std::string& line);
    void parseJob(const std::string& line);
    void parseCmd(const std::string& line);
};

#endif // JOB_GRAPH_H

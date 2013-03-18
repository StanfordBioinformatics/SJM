//
// sjm.cc - simple job manager
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

static char const rcsid[] = "$Id: sjm.cc 213 2008-07-08 20:51:42Z lacroute $";

#include <iostream>
#include <string>

/* don't use sjm's configure variables for TCLAP */
#undef HAVE_CONFIG_H
#include <tclap/CmdLine.h>

#include "job_mon.hh"

using std::cerr;
using std::endl;
using std::string;
using std::vector;

// class for storing command-line options
class Options {
public:
    string jobFile_;		// input file name
    string outFile_;		// output file name
    string logFile_;		// log file name
    bool render_;		// render job graph into dot-format
				// output file instead of submitting jobs
    bool interactive_;		// run as a foreground process instead
				// of in the background
    unsigned dispatchCount_;	// max jobs to dispatch per dispatch interval
    unsigned dispatchInterval_;	// dispatch interval (seconds)
    unsigned pendingJobLimit_;	// max pending jobs
    unsigned runningJobLimit_;	// max running jobs
    unsigned checkJobInterval_;	// interval between checking job status (sec)
    unsigned saveInterval_;	// interval between saving job status (sec)
    unsigned logInterval_;	// interval between logging status (sec)
    unsigned maxStatusErr_;	// give up on a job after this many errors
    string logLevel_;		// log level
    vector<string> mailAddr_;	// email addresses for notification

    Options():
	render_(false),
	interactive_(false),
	dispatchCount_(0),
	dispatchInterval_(0),
	pendingJobLimit_(0),
	runningJobLimit_(0),
	checkJobInterval_(0),
	saveInterval_(0),
	logInterval_(0),
	maxStatusErr_(0)
    {
    }

    void parseArgs(int argc, char **argv);
};

// parse the command-line options
void
Options::parseArgs (int argc, char **argv)
{
    try {
	using TCLAP::CmdLine;
	using TCLAP::SwitchArg;
	using TCLAP::ValueArg;
	using TCLAP::UnlabeledValueArg;
	using TCLAP::MultiArg;
    
	CmdLine cmd("simple job manager", ' ', "$LastChangedRevision: 67 $");
	ValueArg<string> outFile("o", "out", "output file",
				false, "", "filename", cmd);
	ValueArg<string> logFile("l", "log", "log file",
				false, "", "filename", cmd);
	SwitchArg render("r", "render", "render job graph", cmd, false);
	SwitchArg interactive("i", "interactive", "run in foreground",
			      cmd, false);
	ValueArg<unsigned> dispatchCount("", "max_dispatch",
					 "maximum jobs to dispatch per "
					 "dispatch interval",
					 false, 0, "count", cmd);
	ValueArg<unsigned> dispatchInterval("", "dispatch_interval",
					    "dispatch interval",
					    false, 0, "seconds", cmd);
	ValueArg<unsigned> pendingJobLimit("", "max_pending",
					   "maximum number of pending jobs",
					   false, 0, "count", cmd);
	ValueArg<unsigned> runningJobLimit("", "max_running",
					   "maximum number of running jobs",
					   false, 0, "count", cmd);
	ValueArg<unsigned> checkJobInterval("", "check_interval",
					    "interval between checking job "
					    "status",
					    false, 0, "seconds", cmd);
	ValueArg<unsigned> saveInterval("", "save_interval",
					"interval between saving job status",
					false, 0, "seconds", cmd);
	ValueArg<unsigned> logInterval("", "log_interval",
				       "interval between logging status",
				       false, 0, "seconds", cmd);
	ValueArg<unsigned> maxStatusErr("", "max_status_errors",
					"maximum status-collection errors",
					false, 0, "count", cmd);
	ValueArg<string> logLevel("", "log_level", "log level",
				  false, "", "verbose|info|warning|error",
				  cmd);
	MultiArg<string> mailAddr("", "mail", "email address for notification",
				  false, "email_addr", cmd);
	UnlabeledValueArg<string> jobFile("jobfile", "job description file",
					  true, "", "filename", cmd);

	cmd.parse(argc, argv);
	if (argc == 1) {
	    cmd.getOutput()->usage(cmd);
	    exit(0);
	}
	outFile_ = outFile.getValue();
	logFile_ = logFile.getValue();
	render_ = render.getValue();
	interactive_ = interactive.getValue();
	jobFile_ = jobFile.getValue();
	dispatchCount_ = dispatchCount.getValue();
	dispatchInterval_ = dispatchInterval.getValue();
	pendingJobLimit_ = pendingJobLimit.getValue();
	runningJobLimit_ = runningJobLimit.getValue();
	checkJobInterval_ = checkJobInterval.getValue();
	saveInterval_ = saveInterval.getValue();
	logInterval_ = logInterval.getValue();
	maxStatusErr_ = maxStatusErr.getValue();
	logLevel_ = logLevel.getValue();
	mailAddr_ = mailAddr.getValue();
    } catch (TCLAP::ArgException &e) {
	cerr << "sjm: " << e.error() << " for arg " << e.argId() << endl;
	exit(1);
    }
}

int
main (int argc, char **argv)
{
    // parse command-line options
    Options options;
    options.parseArgs(argc, argv);

    JobMonitor jobMon;
    if (options.render_) {
	if (!jobMon.render(options.jobFile_, options.outFile_)) {
	    exit(1);
	}
    } else {
	jobMon.setDispatchCount(options.dispatchCount_);
	jobMon.setDispatchInterval(options.dispatchInterval_);
	jobMon.setPendingJobLimit(options.pendingJobLimit_);
	jobMon.setRunningJobLimit(options.runningJobLimit_);
	jobMon.setCheckJobInterval(options.checkJobInterval_);
	jobMon.setSaveInterval(options.saveInterval_);
	jobMon.setLogInterval(options.logInterval_);
	jobMon.setMaxStatusErr(options.maxStatusErr_);
	if (!options.logLevel_.empty()) {
	    if (options.logLevel_ == "verbose") {
		jobMon.setLogLevel(JobMonitor::LogVerbose);
	    } else if (options.logLevel_ == "info") {
		jobMon.setLogLevel(JobMonitor::LogInfo);
	    } else if (options.logLevel_ == "warning") {
		jobMon.setLogLevel(JobMonitor::LogWarning);
	    } else if (options.logLevel_ == "error") {
		jobMon.setLogLevel(JobMonitor::LogError);
	    } else {
		cerr << "sjm: unknown log level " << options.logLevel_ << endl;
	    }
	}
	if (!jobMon.submit(options.jobFile_, options.outFile_,
			   options.logFile_, options.interactive_,
			   options.mailAddr_)) {
	    exit(1);
	}
    }
    exit(0);
}

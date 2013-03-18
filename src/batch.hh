//
// batch.hh - sjm batch system interface
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
// $Id: batch.hh 171 2008-04-17 22:57:24Z lacroute $
//

#ifndef BATCH_H
#define BATCH_H

#include <map>

#include "job_graph.hh"

// interface definition for all batch systems
class BatchSystem {
public:
    virtual ~BatchSystem() { }
    virtual void init() = 0;
    virtual bool submit(Job& job) = 0;
    virtual void kill(Job& job) = 0;
    virtual bool checkStatus(Job& job) = 0;
    virtual bool canWait() = 0;
    virtual Job* wait(JobGraph& jobs, unsigned timeout_sec) = 0;
    virtual void cleanup() = 0;
};

// stubs for testing without a batch system
class StubBatchSystem: public BatchSystem {
public:
    typedef unsigned JobIdType;

    StubBatchSystem();
    virtual void init() { }
    virtual bool submit(Job& job);
    virtual void kill(Job& job);
    virtual bool checkStatus(Job& job);
    virtual bool canWait();
    virtual Job* wait(JobGraph& jobs, unsigned timeout_sec);
    virtual void cleanup();

private:
    class JobState {
    public:
	unsigned submitTime;	// submission time in seconds
	unsigned schedTime;	// scheduler decision time in seconds
	unsigned startTime;	// start time in seconds
	unsigned endTime;	// end time in seconds
    };
    typedef std::map<JobIdType,JobState> JobMap;

    unsigned nextJobId_;
    JobMap jobs_;

    unsigned currentTime();
};

#endif // BATCH_H

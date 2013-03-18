//
// sge.hh - sjm interface to Sun Grid Engine job submission API
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
// $Id: sge.hh 169 2008-04-15 02:17:46Z lacroute $
//

#ifndef SGE_H
#define SGE_H

#include <drmaa.h>

#include "job_graph.hh"
#include "batch.hh"

class SgeBatchSystem: public BatchSystem {
public:
    typedef unsigned long long JobIdType;

    SgeBatchSystem();
    virtual void init();
    virtual bool submit(Job& job);
    virtual void kill(Job& job);
    virtual bool checkStatus(Job& job);
    virtual bool canWait();
    virtual Job* wait(JobGraph& jobs, unsigned timeout_sec);
    virtual void cleanup();

private:
    bool parseUsageString(const char* usage, const char* name,
			  double* value);
};

#endif // SGE_H

#include "inter.h"
#include <ctime>

MyJob::MyJob(JobID jobId, job_t::type jobType, int32_t k, double duration, 
            double slowDuration, double arriveTime) {
    this->jobId = jobId;
    this->jobType = jobType;
    this->k = k;
    this->duration = duration;
    this->slowDuration = slowDuration;
    this->arriveTime = arriveTime;
    this->startTime = -1;
}

void MyJob::Start(std::set<int32_t> & machines, bool isPrefered) {
    time(&this->startTime);
    this->isPrefered = isPrefered;
    this->assignedMachines = machines;
}

void MyJob::FreeMachine(int machineID) {
    this->assignedMachines.erase(machineID);
}

bool MyJob::IsFinished() {
    if (this->assignedMachines.size() == 0)
        return true;
    else
        return false;
}



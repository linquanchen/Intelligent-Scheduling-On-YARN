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

MyJob::MyJob(MyJob* job) {
    jobId = job->jobId;
    jobType = job->jobType;
    k = job->k;
    duration = job->duration;
    slowDuration = job->slowDuration;
    arriveTime = job->arriveTime;
    startTime = job->startTime;
    isPrefered = job->isPrefered;
    assignedMachines = job->assignedMachines;
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

double MyJob::CalUtility(bool isPrefered) {
    double waitingTime = difftime(time(), arriveTime);
    double runningTime = isPrefered ? duration : slowDuration;
    double result = 1200 - waitingTime - runningTime;
    return result < 0 ? 0 : result;
}

time_t MyJob::GetFinishedTime() {
    double runningTime = isPrefered ? duration : slowDuration;
    return startTime + (int)runningTime;
}

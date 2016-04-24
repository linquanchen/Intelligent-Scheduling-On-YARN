/** @file MyJob.cpp
 *  @brief This file contains implementation of Job Class.
 *
 *  @author Ke Wu <kewu@andrew.cmu.edu>
 *  @author Linquan Chen <linquanc@andrew.cmu.edu>
 *
 *  @bug No known bugs
 *
 */

#include "inter.h"
#include <ctime>
#include <stdio.h>

/** @brief Constructor with several params.
 *  @param jobId The id of the job
 *  @param jobtype The type of the job
 *  @param k the number of machines the job need
 *  @param duration the fast duration the job need to run
 *  @param slowduration the slowest duration the job need to run
 *  @param arrivetime the arrive time of the job
 */
MyJob::MyJob(JobID jobId, job_t::type jobType, int32_t k, double duration, 
            double slowDuration, time_t arriveTime) {
    this->jobId = jobId;
    this->jobType = jobType;
    this->k = k;
    this->duration = duration;
    this->slowDuration = slowDuration;
    this->arriveTime = arriveTime;


    this->startTime = -1;
}

/** @brief Constructor with another job.
 *  @param job The job that constructor based on
 */
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

/** @brief Start the job with allocated machines.
 *  @param machines Machines that allocated to the job
 *  @param isPrefered true if the job running in the preferred resources.
 */
void MyJob::Start(std::set<int32_t> & machines, bool isPrefered) {
    time(&this->startTime);
    this->isPrefered = isPrefered;
    this->assignedMachines = machines;
}

/** @brief Free the machine that allocated to the job.
 *  @param machineID The id of the free machine 
 */
void MyJob::FreeMachine(int machineID) {
    this->assignedMachines.erase(machineID);
}

/** @brief Check if the job is finished or not.
 *  @return true if the job finished, else return false
 */
bool MyJob::IsFinished() {
    if (this->assignedMachines.size() == 0)
        return true;
    else
        return false;
}

/** @brief Calculate the utiltity of the job.
 *  @param curTime the current time
 *  @param isPrefered true if the job running in the preferred resources. 
 *  @return the utiltiy of the job
 */
double MyJob::CalUtility(time_t curTime, bool isPrefered) {
    double waitingTime = difftime(curTime, arriveTime);
    waitingTime = waitingTime < 0 ? 0 : waitingTime;
    double runningTime = isPrefered ? duration : slowDuration;
    double result = 1200 - waitingTime - runningTime;
    return result < 0 ? 0 : result;
}

/** @brief Get the finished time of the job.
 *  @return finished time
 */
time_t MyJob::GetFinishedTime() {
    double runningTime = isPrefered ? duration : slowDuration;
    return startTime + (int)runningTime;
}

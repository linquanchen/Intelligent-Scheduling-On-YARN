/** @file Cluster.cpp
 *  @briei This file contains implementation of the Cluster, which can be used to 
 *         get the scheduling decision based on N-step search.
 *
 *  @author Ke Wu <kewu@andrew.cmu.edu>
 *  @author Linquan Chen <linquanc@andrew.cmu.edu>
 *
 *  @bug No know bugs.
 */
#include "inter.h"
#include <stdio.h>

/** @brief Construcor. Create a snapshot of the currrent scheduler
 *  @param racks The vector of Machines of every rack
 *  @param pendingjoblist The list contains all pending jobs
 *  @param runningjoblist The priority queue contains all running jobs
 *  @param maxmachinesperrack The number of max machines
 *  @param isSoft true if the policy is soft, else false
 */
Cluster::Cluster(std::vector<std::vector<MyMachine> > & racks, 
            std::list<MyJob*> & pendingJobList,
            std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> & runningJobList,
            int maxMachinesPerRack, bool isSoft) {

    this->racks = racks;
    
    // Copy the pending jobs to the cluster.
    for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                                             i != pendingJobList.end(); ++i) {
        MyJob* newJob = new MyJob(*i);
        this->pendingJobList.push_back(newJob);
    }
    
    // Copy the running jobs to the Cluster.
    std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> tmp = runningJobList;
    while(!tmp.empty()) {
        MyJob* tmpJob = tmp.top();
        tmp.pop();

        MyJob* newJob = new MyJob(tmpJob);
        this->runningJobList.push(newJob);
        for (std::set<int32_t>::iterator it=tmpJob->assignedMachines.begin(); 
                                    it!=tmpJob->assignedMachines.end(); ++it) {
            GetMachineByID(*it)->AssignJob(newJob);
        }
    }

    this->maxMachinesPerRack = maxMachinesPerRack;
    this->isSoft = isSoft;
}

/** @brief Free resources of the cluster object. */
void Cluster::Clear() {
    // Clear the pending jobs.
    for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                                             i != pendingJobList.end(); ++i) {
        delete (*i);
    }
    
    // Clear the running jobs.
    while(!runningJobList.empty()) {
        MyJob* tmpJob = runningJobList.top();
        runningJobList.pop();
        delete tmpJob;
    }
}

/** @brief Get the running decision to acheieve the highest utility using n-step search algorithm.
 *  @return For each vector, 0 is jobID, 1 indicates if is prefered, 2...n is machine ID
 */
std::vector<std::vector<int> > Cluster::Schedule() {

    int counter = SEARCH_STEP;
    std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> tmpRunningJobList = runningJobList;
    
    // searchEndJobId == -1 means no running job, search should be finished immediately
    int searchEndJobId = -1;

    // find the searching end job, this job limits the maximum search steps. It should be the nth running job
    // in the current runningJobList
    while (counter-- > 0 && !tmpRunningJobList.empty()) {
        searchEndJobId = (int)tmpRunningJobList.top()->jobId;
        tmpRunningJobList.pop();
    }

    double resultUtility;

    // starting searching, with maximum EXTRA_SEARCH_STEP steps, searching shoud end when encounter searchEndJobId
    return Search(EXTRA_SEARCH_STEP, searchEndJobId, time(NULL), resultUtility);
}

/** @brief Get the machine based on the machine ID.
 *  @param id the machine id
 *  @return the machine correspond to the id
 */
MyMachine* Cluster::GetMachineByID(unsigned int id) {
    unsigned int rackID = 0;
    while (id >= racks[rackID].size()) {
        id -= racks[rackID].size();
        rackID++;
    }
    return &(racks[rackID][id]);
}

/** @brief Get the total number of free machines */
int Cluster::GetFreeMachinesNum() {
    int count = 0;
    for (unsigned int i = 0; i < racks.size(); i++)
        for (unsigned int j = 0; j < racks[i].size(); j++)
            if (racks[i][j].IsFree())
                count++;
    return count;
}

/** @brief Mark a set of machines as allocated
 *  @param machines The set of machines that will be marked as allocated
 */
void Cluster::AllocateMachinesToJob(MyJob* job, std::set<int32_t> & machines, bool isPrefered) {
    for (std::set<int32_t>::iterator it=machines.begin(); 
                                                it!=machines.end(); ++it) {
        GetMachineByID(*it)->AssignJob(job);
    }

    job->Start(machines, isPrefered);
}

/** @brief Free machines belong to the job
 *  @param job the freed job
 */
void Cluster::FreeMachinesByJob(MyJob* job) {
    std::set<int32_t> tmpMachines = job->assignedMachines;

    for (std::set<int32_t>::iterator it=tmpMachines.begin(); 
                                    it!=tmpMachines.end(); ++it) {
        GetMachineByID(*it)->Free();
        job->assignedMachines.erase(*it);
    }
}

/** @brief Get free VM number of every rack
 *  @return free VM number of every rack
 */
std::vector<int> Cluster::GetFreeMachines() {
    std::vector<int> freeMachines; 
    for (unsigned int i = 0; i < racks.size(); i++) {
        int num = 0;
        for (unsigned int j = 0; j < racks[i].size(); j++) {
            if (racks[i][j].IsFree()) {
                num++;
            }
        }
        freeMachines.push_back(num);
    }
    return freeMachines;
}

/** @brief Get k free VMs from rack index
 *  @param machines The set of machines to store k free VMs
 *  @param k The number of machines that the is asking
 *  @param index The rack to get k VMs
 */
void Cluster::GetMachineByRack(std::set<int> & machines, int k, int index) {
    for (unsigned int i = 0; k != 0 && i < racks[index].size(); i++) {
        if (racks[index][i].IsFree()) {
            machines.insert(racks[index][i].machineID);
            k--;
        }
    }
    if (k != 0) {
        dbg_printf("Something wrong with AddJobsByRack()\n");
    }
}


/** @brief Get the rack number which has minimum free VMs
 *  @param freemachines the free VM number of every rack
 *  @return the index of the rack
 */
int Cluster::FindMinRack(std::vector<int> & freeMachines) {
    int min = maxMachinesPerRack;
    int index = -1;
    for (unsigned int i = 0; i < freeMachines.size(); i++) {
        int num = freeMachines[i];
        if (num != 0 && num <= min) {
            min = num;
            index = i;
        }
    }
    return index;
}

/** @brief Get (preferred configuration) machines for MPI job.
 *  @param machines The set of machines that will be allocated to the job 
 *  @param k The number of machines that the job is asking 
 *  @return true if on job's preferred allocation, else false
 */
bool Cluster::GetMachinesForMPI(std::set<int> & machines, int k) {
    std::vector<int> freeMachines = GetFreeMachines();
    
    int min = maxMachinesPerRack + 1;
    int index = -1;
    for (unsigned int i = 1; i < freeMachines.size(); i++) {
        int num = freeMachines[i];
        if (num >= k) {
            if (num < min) {
                min = num;
                index = i;
            }
        }        
    }

    /* When one rack(no GPU) has enough VMs for MPI jobs */
    if (index != -1) {
        GetMachineByRack(machines, k, index);    
        return true;
    }
    
    /* When GPU rack has enough VMs for MPI jobs */
    if (freeMachines[0] >= k) {
        GetMachineByRack(machines, k, 0);
        return true;
    }
    
    while (k != 0) {
        index = FindMinRack(freeMachines);
        if (freeMachines[index] <= k) {
            GetMachineByRack(machines, freeMachines[index], index);
            k -= freeMachines[index];
            freeMachines[index] = 0;
        }
        else {
            GetMachineByRack(machines, k, index);
            k = 0;
        }
    }
    return false;
}

/** @brief Get (preferred configuration) machines for GPU job.
 *  @param machines The set of machines that will be allocated to the job 
 *  @param k The number of machines that the job is asking 
 *  @return true if on job's preferred allocation, else false
 */
bool Cluster::GetMachinesForGPU(std::set<int> & machines, int k) {
    std::vector<int> freeMachines = GetFreeMachines();
     
    /* When the GPU rack has enough VMs for GPU jobs */
    if (freeMachines[0] >= k) {
        GetMachineByRack(machines, k, 0);
        return true;
    }
    
    int index;
    while (k != 0) {
        index = FindMinRack(freeMachines);
        if (freeMachines[index] <= k) {
            GetMachineByRack(machines, freeMachines[index], index);
            k -= freeMachines[index];
            freeMachines[index] = 0;
        }
        else {
            GetMachineByRack(machines, k, index);
            k = 0;
        }
    }
    return false;
} 

/** @brief Get machines for specific job.
 *  @param machines The set of machines that will be allocated to the job 
 *  @param jobType The type of the job
 *  @param k The number of machines that the job is asking 
 *  @return true if on job's preferred allocation, else false
 */
bool Cluster::GetBestMachines(job_t::type jobType, int k, 
                                            std::set<int32_t> &machines) {
    switch(jobType) {
        case job_t::JOB_MPI:
            return GetMachinesForMPI(machines, k);
        case job_t::JOB_GPU:   
            return GetMachinesForGPU(machines, k);
        default:{
            dbg_printf("Unknown job type:%d\n", jobType);
            return false;
        }
    } 
}

/** @brief Search for the running decision to get highest utility.
 *  @param step The number of search step
 *  @param searchEndJobId The job which is the end of the search process
 *  @param curTime "Current" time of simulation
 *  @param resultUtility The total utility of each search process, this is also a return value
 *  @return For each vector, 0 is jobID, 1 indicates if is prefered, 2...n is machine ID
 */
std::vector<std::vector<int> > Cluster::Search(int step, int searchEndJobId, time_t curTime, double & resultUtility) {
    if (step == 0 && searchEndJobId != -1) {
        // no search step left, can not search decisions, just go to the last search step
        MyJob* finishedJob = runningJobList.top();
        runningJobList.pop();

        int nextSearchEndJobId = ((int)finishedJob->jobId == searchEndJobId) ? -1 : searchEndJobId;
        
        FreeMachinesByJob(finishedJob);
        
        time_t nextTime = curTime;
        if (difftime(finishedJob->GetFinishedTime(), curTime) > 0)
            nextTime = finishedJob->GetFinishedTime();

        return Search(step, nextSearchEndJobId, nextTime, resultUtility);
    }

    // potentialRunningJobs is used to store the (maximum possible) jobs that can be scheduled with current 
    // resources, the first element in potentialRunningJobs is the job with the highest utiltiy
    std::vector<MyJob*> potentialRunningJobs;
    // potentialUtility stores the utilities gained from each job in potentialRunningJobs
    std::vector<double> potentialUtility;


    // try to schedule as many jobs as possible with current resources, following the utility greedy policy
    while (true) {
        std::list<MyJob*>::iterator bestJobIter;
        double maxUtility = -1, tmpUtility;
        std::set<int32_t> bestMachines, tmpMachines;
        bool isBestisPrefered;

        int freeMachineNum = GetFreeMachinesNum();

        for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                                                    i != pendingJobList.end(); ++i){
            if (freeMachineNum >= (*i)->k) {
                // try to find the best (preferred) machine allocation
                bool isPrefered = 
                        GetBestMachines((*i)->jobType, (*i)->k, tmpMachines);

                if (!isPrefered && !isSoft) {
                    // for hard policy, job remains pending if preference can not be satisfied
                    tmpMachines.clear();
                    continue;
                }
                
                tmpUtility = (*i)->CalUtility(curTime, isPrefered);

                // if find a job with larger utility, update schedule solution
                if (maxUtility < tmpUtility) {
                    bestJobIter = i;
                    maxUtility = tmpUtility;
                    bestMachines = tmpMachines;
                    isBestisPrefered = isPrefered;
                }  
                tmpMachines.clear();
            }
        }

        // find a runnable job with current left resources, add it to potentialRunningJobs
        if (maxUtility > 0) {
            AllocateMachinesToJob(*bestJobIter, bestMachines, isBestisPrefered);
            potentialRunningJobs.push_back(*bestJobIter);
            pendingJobList.erase(bestJobIter);

            potentialUtility.push_back(maxUtility);
        } else
            // not job can be satisfied with current left resource
            break;
    }

    std::vector<std::vector<int> > result = constructResult(potentialRunningJobs);
    
    // Get the current total utility, which is the total utility if all potentialRunningJobs are really scheduled
    double curUtility = 0;
    for (std::vector<double>::iterator it = potentialUtility.begin() ; it != potentialUtility.end(); ++it)
        curUtility += *it;

    // searchEndJobId == -1, should end search immediately 
    if (searchEndJobId == -1) {
        resultUtility = curUtility;
        return result;
    }


    // try to delay one or more jobs in potentialRunningJobs (i.e. don't run all jobs even some resources are available)
    resultUtility = -1;
    while(true) {
        
        // add all jobs in potentialRunningJobs to runningJobList
        std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> tmpRunningJobList = runningJobList;
        for (std::vector<MyJob*>::iterator it=potentialRunningJobs.begin(); 
                                        it != potentialRunningJobs.end(); ++it){
            tmpRunningJobList.push(*it);
        }

        // Based on the current the allocated decision, simulate and schedule the next allocated decision 
        // and get the total utility.
        double nextResultUtility;
        Cluster cluster(racks, pendingJobList, tmpRunningJobList, maxMachinesPerRack, isSoft);
        cluster.SimulateNext(step, searchEndJobId, curTime, nextResultUtility);
        cluster.Clear();

        // Compare the current schedule utility with the last best one.
        if (curUtility + nextResultUtility > resultUtility) {
            resultUtility = curUtility + nextResultUtility;
            result = constructResult(potentialRunningJobs);
        }

        if (potentialRunningJobs.empty())
            break;
        
        // delay the last potential running job (add it back to pendingJobList, free resources)
        MyJob* myjob = potentialRunningJobs.back();
        potentialRunningJobs.pop_back();
        curUtility -= potentialUtility.back();
        potentialUtility.pop_back();
        
        pendingJobList.push_back(myjob);
        FreeMachinesByJob(myjob);
    }

    return result;
}

/** @brief Simulate the next seasrch step based on the last time.
 *  @param step The number of search step
 *  @param searchEndJobId The job which is the end of the search process
 *  @param curTime The "current" time of simulation
 *  @param resultUtility The total utility of each search process
 *  @return For each vector, 0 is jobID, 1 indicates if is prefered, 2...n is machine ID
 */
std::vector<std::vector<int> > Cluster::SimulateNext(int step, int searchEndJobId, time_t curTime, 
                                                                                            double & resultUtility) {
    MyJob* finishedJob = runningJobList.top();
    runningJobList.pop();
    
    // Check if it is the end job
    int nextSearchEndJobId = ((int)finishedJob->jobId == searchEndJobId) ? -1 : searchEndJobId;

    FreeMachinesByJob(finishedJob);

    // the next time of simulated scheduling will happen when the job at the head of the runningJobList finish runnning
    time_t nextTime = curTime;
    if (difftime(finishedJob->GetFinishedTime(), curTime) > 0)
        nextTime = finishedJob->GetFinishedTime();

    // Start the next step search based on the last decision.
    return Search(step-1, nextSearchEndJobId, nextTime, resultUtility);
}

/** @brief Get job info and allocated machines from the potential jobs.
 *  @jobs The vector of jobs that will run potentially
 *  @return For each vector, 0 is jobID, 1 indicates if is prefered, 2...n is machine ID
 */
std::vector<std::vector<int> > Cluster::constructResult(std::vector<MyJob*> & jobs) {
    std::vector<std::vector<int> > result;

    for (std::vector<MyJob*>::iterator it=jobs.begin(); 
                                        it != jobs.end(); ++it) {
        std::vector<int> tmp;
        tmp.push_back((*it)->jobId);
        tmp.push_back((*it)->isPrefered);
        for (std::set<int32_t>::iterator i=(*it)->assignedMachines.begin(); 
                                        i != (*it)->assignedMachines.end(); ++i) {
            tmp.push_back(*i);
        }

        result.push_back(tmp);
    }
    return result;
}



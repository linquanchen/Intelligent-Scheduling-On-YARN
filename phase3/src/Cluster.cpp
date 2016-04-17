#include "inter.h"

    
Cluster::Cluster(std::vector<std::vector<MyMachine> > & racks, 
            std::list<MyJob*> & pendingJobList,
            std::priority_queue<MyJob*, vector<MyJob*>, JobComparison> & runningJobList,
            int maxMachinesPerRack) {

    this->racks = racks;

    for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                                             i != pendingJobList.end(); ++i) {
        MyJob* newJob = new MyJob(*i);
        this->pendingJobList.push_back(newJob);
    }

    std::priority_queue<MyJob*> tmp = runningJobList;
    while(!tmp.empty()) {
        MyJob* tmpJob = tmp.pop();

        MyJob* newJob = new MyJob(tmpJob);
        this->runningJobList.push(newJob);
        for (std::set<int32_t>::iterator it=tmpJob->assignedMachines.begin(); 
                                    it!=tmpJob->assignedMachines.end(); ++it) {
            GetMachineByID(*it)->AssignJob(newJob);
        }
    }

    this->maxMachinesPerRack = maxMachinesPerRack;
}


Cluster::Clear() {
    for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                                             i != pendingJobList.end(); ++i) {
        delete (*i);
    }

    while(!runningJobList.empty()) {
        MyJob* tmpJob = runningJobList.pop();
        delete tmpJob;
    }
}

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
        GetMachineByID(*it)->assignJob(job);
    }

    job->Start(machines, isPrefered);
}

void Cluster::FreeMachinesByJob(MyJob* job) {
    std::set<int32_t> tmpMachines = job->assignedMachines;

    for (std::set<int32_t>::iterator it=tmpMachines.begin(); 
                                    it!=tmpMachines.end(); ++it) {
        GetMachineByID(*it)->Free();
        job->assignedMachines->erase(*it);
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

/** @brief Get k VMs from rack index
 *  @param machines The set of machines to store k VMs
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

/** @brief Get machines for MPI job.
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

/** @brief Get machines for GPU job.
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

std::vector<std::vector<int> > Cluster::Schedule() {
    std::vector<MyJob*> potentialRunningJobs;
    std::vector<std::vector<int> > result;
    std::vector<double> potentialUtility;
    double resultUtility;

    // try to allocate resources for one or more jobs
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
                
                tmpUtility = (*i)->CalUtility(time(), isPrefered);

                // if find a job with larger utility, update schedule solution
                if (maxUtility < tmpUtility) {
                    bestJobIter = i;
                    maxUtility = tmpUtility;
                    bestMachines = tmpMachines;
                    isBestisPrefered = isPrefered;
                    tmpMachines.clear();
                }  
            }
        }

        if (maxUtility > 0) {
            AllocateMachinesToJob(*bestJobIter, bestMachines, isBestisPrefered);
            potentialRunningJobs.push_back(*bestJobIter);
            pendingJobList.erase(bestJobIter);

            potentialUtility.push_back(maxUtility);
        } else
            // not job can be satisfied with current left resource
            break;
    }

    // initial result is allocate resources for all potential running jobs 
    result = constructResult(potentialRunningJobs);
    // initial resultUtility is the total utility that allocate resources for all potential running jobs 
    resultUtility = 0;
    for (std::vector<double>::iterator it = potentialUtility.begin() ; it != potentialUtility.end(); ++it)
        resultUtility += *it;

    double curUtility = resultUtility;
    int delayJobNum = 0;
    while(!potentialRunningJobs.empty()) {
        // delay the last potential running job
        delayJobNum++;

        MyJob* myjob = potentialRunningJobs.pop_back();
        curUtility -= potentialUtility.pop_back();
        
        pendingJobList.push_back(myjob);
        FreeMachinesByJob(myjob);

        std::priority_queue<MyJob*> tmpRunningJobList = runningJobList;
        for (std::vector<MyJob*>::iterator it=potentialRunningJobs.begin(); 
                                        it != potentialRunningJobs.end(); ++it){
            tmpRunningJobList.push(*it);
        }

        Cluster tmpCluster(racks, pendingJobList, tmpRunningJobList, maxMachinesPerRack);
        addedUtility = tmpCluster.CalAddedUtility(delayJobNum);
        tmpCluster.Clear();

        if (curUtility + addedUtility > resultUtility) {
            resultUtility = curUtility + addedUtility;
            result = constructResult(potentialRunningJobs);
        }

    }

    return result;
}

double Cluster::CalAddedUtility(int delayJobNum) {
    time_t curTime = time();
    double resultUtility = 0, penaltyUtility = 0;
    while (!RunningJobList.empty()) {
        MyJob* finishedJob = RunningJobList.pop();
        FreeMachinesByJob(finishedJob);
        
        double elapsedTime = difftime(finishedJob->GetFinishedTime(), curTime);
        if (elapsedTime > 0) {
            penaltyUtility -= delayJobNum * elapsedTime;
            curTime = finishedJob->GetFinishedTime();
        }


        double addedUtility = 0;
        std::vector<MyJob*> tmpRunningJobs;
        // calculate addedUtility when delay k jobs to later time to run
        for (int k = 0; k < delayJobNum; k++) {
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
                    
                    tmpUtility = (*i)->CalUtility(curTime, isPrefered);

                    // if find a job with larger utility, update schedule solution
                    if (maxUtility < tmpUtility) {
                        bestJobIter = i;
                        maxUtility = tmpUtility;
                        bestMachines = tmpMachines;
                        isBestisPrefered = isPrefered;
                        tmpMachines.clear();
                    }  
                }
            }

            if (maxUtility > 0) {
                AllocateMachinesToJob(*bestJobIter, bestMachines, isBestisPrefered);
                tmpRunningJobs.push_back(*bestJobIter);
                pendingJobList.erase(bestJobIter);

                addedUtility += maxUtility;
            } else
                // impossible, something wrong
                dbg_printf("error in CalAddedUtility!");
        }

        if (resultUtility < penaltyUtility + addedUtility)
            resultUtility = penaltyUtility + addedUtility;

        // free temporty allocated machines
        for (std::vector<MyJob*>::iterator it=tmpRunningJobs.begin(); 
                                        it != tmpRunningJobs.end(); ++it) {
            FreeMachinesByJob(*it);
            pendingJobList.push_back(*it);
        }
    }
}

std::vector<std::vector<int> > constructResult(std::vector<MyJob*> & jobs) {
    std::vector<std::vector<int> > result;

    for (std::vector<MyJob*>::iterator it=jobs.begin(); 
                                        it != jobs.end(); ++it) {
        std::vector<int> tmp;
        tmp.push_back((*it)->jobId);
        tmp.push_back((*it)->isPrefered);
        for (std::vector<int32_t>::iterator i=(*it)->assignedMachines.begin(); 
                                        i != (*it)->assignedMachines.end(); ++i) {
            tmp.push_back(*i);
        }

        result.push_back(tmp);
    }

    return result;
}



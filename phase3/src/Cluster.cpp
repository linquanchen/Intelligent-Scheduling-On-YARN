#include "inter.h"

    
Cluster::Cluster(std::vector<std::vector<MyMachine> > & racks, 
            std::list<MyJob*> & pendingJobList,
            std::list<MyJob*> & runningJobList,
            int maxMachinesPerRack) {

    this->racks = racks;
    

    this->maxMachinesPerRack = maxMachinesPerRack;
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
        job->erase(*it);
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

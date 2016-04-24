/** @file MyMachine.cpp
 *  @brief This file contains implementation of the Machine class.
 *
 *  @author Ke Wu <kewu@andrew.cmu.edu>
 *  @author Linquan Chen <linquanc@andrew.cmu.edu>
 *
 *  @bug No know bugs
 */

#include "inter.h"

/** @brief Consructor with machineID. */
MyMachine::MyMachine(int machineID) {
    this->machineID = machineID;
    this->belongedJob = NULL;
}

/** @brief Assign a job to the machine.
 *  @param job the assigned job
 */
void MyMachine::AssignJob(MyJob* job) {
    this->belongedJob = job;
}

/** @brief Free the machine: remove the assigned job. */
void MyMachine::Free() {
    this->belongedJob = NULL;
}

/** @brief Check if the machine is free or not.
 *  @return true if the job is free, else return false
 */
bool MyMachine::IsFree() {
    return this->belongedJob == NULL;
}

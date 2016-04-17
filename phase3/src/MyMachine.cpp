#include "inter.h"
    
MyMachine::MyMachine(int machineID) {
    this->machineID = machineID;
    this->belongedJob = NULL;
}

void MyMachine::AssignJob(MyJob* job) {
    this->belongedJob = job;
}

void MyMachine::Free() {
    this->belongedJob = NULL;
}

bool MyMachine::IsFree() {
    return this->belongedJob == NULL;
}

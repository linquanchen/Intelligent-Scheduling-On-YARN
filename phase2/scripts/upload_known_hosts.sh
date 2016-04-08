#!/bin/bash

# Upload knows_hosts to all slave vms. 
# Ps: Download the known_hosts to master from VM h0r0

username="kewu"

declare -a all_ips=("10.10.2.10" "10.10.2.11" "10.10.2.12" "10.10.2.13" "10.10.2.14" "10.10.2.15" "10.10.3.10" "10.10.3.11" "10.10.3.12" "10.10.3.13" "10.10.3.14" "10.10.3.15" "10.10.4.10" "10.10.4.11" "10.10.4.12" "10.10.4.13" "10.10.4.14" "10.10.4.15" "10.10.5.10" "10.10.5.11" "10.10.5.12" "10.10.5.13" "10.10.5.14" "10.10.5.15")
for i in "${all_ips[@]}"
do
    scp known_hosts $username@$i:~/.ssh/known_hosts
    echo $i
done

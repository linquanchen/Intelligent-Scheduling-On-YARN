#!/bin/bash

username="kewu"

declare -a all_ips=("10.10.1.10" "10.10.2.10" "10.10.2.11" "10.10.2.12" "10.10.2.13" "10.10.2.14" "10.10.2.15" "10.10.3.10" "10.10.3.11" "10.10.3.12" "10.10.3.13" "10.10.3.14" "10.10.3.15" "10.10.4.10" "10.10.4.11" "10.10.4.12" "10.10.4.13" "10.10.4.14" "10.10.4.15" "10.10.5.10" "10.10.5.11" "10.10.5.12" "10.10.5.13" "10.10.5.14" "10.10.5.15")
for i in "${all_ips[@]}"
do
    scp ~/.ssh/id_dsa.pub $username@$i:~/.ssh/id_dsa.pub
    scp ~/.ssh/id_dsa $username@$i:~/.ssh/id_dsa
    echo $i
done

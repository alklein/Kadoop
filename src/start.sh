#!/bin/bash

USERNAME="andreakl"

HOME="DS/Kadoop/src"

CD_SCRIPT="cd $HOME ; "
NN_SCRIPT="java DFS/NameNode"
DN_SCRIPT="java DFS/DataNode"
MR_SCRIPT="java MR/Master"
CN_SCRIPT="java MR/ComputeNode"

python print_master_node.py | while read N_NODE
do
    echo "${CD_SCRIPT} ${NN_SCRIPT}" | ssh ${USERNAME}@${N_NODE} /bin/bash &
done

sleep 3

python print_worker_nodes.py | while read D_NODE
do
    echo "${CD_SCRIPT} $DN_SCRIPT" | ssh $USERNAME@$D_NODE /bin/bash &
done



python print_master_node.py | while read M_NODE
do
    echo "${CD_SCRIPT} ${MR_SCRIPT}" | ssh ${USERNAME}@${M_NODE} /bin/bash &
done

sleep 3

python print_worker_nodes.py | while read C_NODE
do
    echo "${CD_SCRIPT} $CN_SCRIPT" | ssh $USERNAME@$C_NODE /bin/bash &
done
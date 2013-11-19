SHUTDOWN="ps aux | grep 'java DFS/NameNode' | awk '{print "'$2'"}' | xargs kill; exit"
USERNAME="andreakl"

python print_master_node.py | while read N_NODE
do
    echo "${SHUTDOWN}" | ssh ${USERNAME}@${N_NODE} /bin/bash &
    echo "NameNode has been shut down."
done
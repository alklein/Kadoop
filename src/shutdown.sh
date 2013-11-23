SHUTDOWN_NN="ps aux | grep 'java DFS/NameNode' | awk '{print "'$2'"}' | xargs kill; exit"
SHUTDOWN_MR="ps aux | grep 'java MR/Master' | awk '{print "'$2'"}' | xargs kill; exit"
USERNAME="andreakl"

python print_master_node.py | while read NODE
do
    echo "${SHUTDOWN_NN}" | ssh ${USERNAME}@${NODE} /bin/bash &
    echo "NameNode has been shut down."
done

python print_master_node.py | while read NODE
do
    echo "${SHUTDOWN_MR}" | ssh ${USERNAME}@${NODE} /bin/bash &
    echo "Master has been shut down."
done
#!/bin/bash
hdtQueue="/scratch/lodlaundromat/tmp/hdtQueue.txt"
#touch /home/lodlaundromat/wm-callback.touch
[ -z "$1" ] && echo "No dataset provided as argument" && exit 1;

date >> /home/lodlaundromat/wm-callback.touch
echo $@ >> /home/lodlaundromat/wm-callback.touch;



echo "Generating HDT file ($1)"
makeHdt $1

#queue hdt file for ldf update
echo $1 >> $hdtQueue;

#analyze directory
echo "Creating C-LOD file ($1)"
streamDataset $1
createModel $1
storeModel $1

exit 0;

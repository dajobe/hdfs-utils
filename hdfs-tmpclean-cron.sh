#!/bin/bash
# Tidy Hadoop /tmp of old files (> 7 days)
#
# Must run as user with admin access to hadoop ('hdfs' typically)

PATH=/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export PATH

# Logs get written here max of $MAX_DAYS
LOG_DIR=/var/log/hdfs-tmpclean

MAX_LOG_DAYS=30

# Hadoop V2
HDFS_OIV_COMMAND="hdfs oiv"
# Hadoop V1
# HDFS_OIV_COMMAND="hadoop oiv"

# Glob to list fsimage files (or just a filename)
HDFS_FSIMAGE_GLOB=/data1/hadoop/hdfs/snn/current/fsimage*

HDFS_TMPCLEAN_BATCH_SIZE=50

HDFS_TMPCLEAN_MAX_FILES=500


#######################

program=`basename $0`

mkdir -p $LOG_DIR
cd $LOG_DIR

today=$(date --iso-8601)

LOG=$today.log

(
    exec >> $LOG 2>&1
    
    echo "$program: Starting at $(date)"
    delimited_tmp=$(mktemp)
    # Exclude .md5 checksum files from fsimage glob
    fsimage_file=$(ls -1tr $HDFS_FSIMAGE_GLOB | grep -v md5 | head -1)

    $HDFS_OIV_COMMAND -i $fsimage_file -o $delimited_tmp -p Delimited

    just_tmp=$(mktemp)
    grep '^/tmp' $delimited_tmp > $just_tmp

    hdfs-tmpclean.py -b $HDFS_TMPCLEAN_BATCH_SIZE -m $HDFS_TMPCLEAN_MAX_FILES $just_tmp

    rm -f $just_tmp $delimited_tmp
    echo "$program: Ending at $(date)"
)

rm -f latest
ln -s $LOG latest

find . -type f -mtime +$MAX_LOG_DAYS -print | xargs rm -f

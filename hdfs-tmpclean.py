#!/usr/bin/python
"""Clean HDFS tmp of old files

Reads a tab delimited fsimage file that is created with a call like
the following on the primary or secondary name node:

    $ hadoop oiv -i /data1/hadoop/hdfs/snn/current/fsimage  -o /tmp/fsimage-delimited.tsv -p Delimited

Then calling this script to delete up to 100 files, 20 at a time:

    $ python hdfs-tmpclean.py -b 20 -m 100 /tmp/fsimage-delimited.tsv

"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime
from datetime import timedelta

# logger for this program
logger = logging.getLogger("hdfs-tmpclean")


def as_int(val):
    """Turn value into int or None"""
    if val == '':
        return None
    try:
        d = int(val)
        val = d
    except ValueError, e:
        logger.debug("Invalid int '%s' - %s", val, str(e))
        val = None
    return val

def as_time(val):
    """Turn value into a datetime or None"""
    if val == '' or val == "1970-01-01 00:00":
        return None
    try:
        (dt_str, time_str) = val.split(' ')
        (year, mon, day) = map(int, dt_str.split('-'))
        (hrs, mins) = map(int, time_str.split(':'))
        val = datetime(year, mon, day, hrs, mins)
    except ValueError, e:
        logger.debug("Invalid date '%s' - %s", val, str(e))
        val = None
    return val


class HadoopFsMetadata(object):
    def __init__(self, line):
        """
        Split fsimage line (TSV) fields

          path: string,
          replication : int,
          modTime: string,
          accessTime: string,
          blockSize : int,
          numBlocks : int,
          fileSize : int,
          NamespaceQuota : int,
          DiskspaceQuota : int,
          perms: string,
          username: string,
          groupname: string

        Example dir:

    /	0	2014-04-28 22:30	1970-01-01 00:00	0	-1	0	9223372036854775807	-1	rwxr-xr-x	user	grp

        Example file:

    /path/to/file	3	2014-05-08 20:30			1	0			rwx------	user	grp
            """

        fields = line.split('\t')
        self.htype = 'file'
        try:
            self.path = fields[0]
            self.replication = as_int(fields[1])
            self.modTime = as_time(fields[2])
            self.accessTime = as_time(fields[3])
            self.blockSize = as_int(fields[4])
            self.numBlocks = as_int(fields[5])
            self.fileSize = as_int(fields[6])
            self.NamespaceQuota = as_int(fields[7])
            self.DiskspaceQuota = as_int(fields[8])
            self.perms = fields[9]
            self.username = fields[10]
            self.groupname = fields[11]
            if self.numBlocks < 0 or self.replication == 0:
                self.htype = 'dir'
        except Exception, e:
            logger.error("Failed to process line %s - %s" % (line, str(e)))
            raise e

    def __str__(self):
        return "{type} {path} modified {mt} accessed {at} blocksize {bs} blocks {blocks} size {size} perms {perms} user {user} group {group}".format(type=self.htype, path=self.path, repl=self.replication, mt=self.modTime, at=self.accessTime, bs=self.blockSize, blocks=self.numBlocks, size=self.fileSize, perms=self.perms, user=self.username, group=self.groupname)
        # self.replication
        # self.NamespaceQuota
        # self.DiskspaceQuota

    def is_dir(self):
        return self.htype == 'dir'

    def is_file(self):
        return self.htype == 'file'

def tmp_clean(dryrun, hdfs, age, tmp, fsimage,
              max_deletes=None, max_errors=5, batch_size=10):
    """Clean hadoop HDFS temp files over @age days below directory @tmp
    as found in HDFS image file fsimage.

    Stop if @max_deletes reached or @max_errors.

    Batch up to @batch removals at one go

    """

    ago = datetime.now() - timedelta(days=age)
    hdfs = hdfs.split(' ')

    logger.info("Reading fsimage delimited file {name}".format(name=fsimage))
    deletions = 0
    errors = 0
    batch = []
    for line in open(fsimage):
        o = HadoopFsMetadata(line)
        if not o.path.startswith(tmp):
            continue

        logger.debug("Candidate path: {path}".format(path=o.path))
        if not o.is_file() or o.modTime >= ago:
            continue

        if dryrun:
            logger.info("Would delete {type} {path} {mt}".format(type=o.htype,path=o.path,mt=o.modTime))
        else:
            logger.info("Deleting {type} {path} {mt}".format(type=o.htype,path=o.path,mt=o.modTime))

        batch.append(o.path)
        if len(batch) == batch_size:
            cmd = hdfs + [ "-rm" ] + batch
            if dryrun:
                logger.info("Would delete {n} HDFS batch files".format(n=len(batch)))
            else:
                logger.debug("Running HDFS command " + str(cmd))
                try:
                    subprocess.check_call(cmd)
                except subprocess.CalledProcessError, e:
                    logger.debug("HDFS command %s failed - %s", str(cmd), str(e))
                    errors += 1
                    if errors == max_errors:
                        break
            deletions += len(batch)
            batch = []

            if max_deletes is not None and deletions >= max_deletes:
                logger.info("Stopping at max {max} files deleted".format(max=max_deletes))
                break

    # Final check for a pending batch
    if len(batch) > 0:
        cmd = hdfs + [ "-rm" ] + batch
        if dryrun:
            logger.info("Would delete {n} HDFS batch files".format(n=len(batch)))
        else:
            logger.debug("Running HDFS command " + str(cmd))
            try:
                subprocess.check_call(cmd)
            except subprocess.CalledProcessError, e:
                logger.debug("HDFS command %s failed - %s", str(cmd), str(e))
                errors += 1
        deletions += len(batch)

    if dryrun:
        logger.info("Would delete {n} files below {dir}".format(n=deletions, dir=tmp))
    else:
        logger.info("Deleted {n} files below {dir}".format(n=deletions, dir=tmp))


def main():
    """Main method"""

    parser = argparse.ArgumentParser(description='Receive data.')
    parser.add_argument('-d', '--debug',
                        action = 'store_true',
                        default = False,
                        help = 'debug messages (default: False)')
    parser.add_argument('-n', '--dryrun',
                        action = 'store_true',
                        default = False,
                        help = 'dryrun (default: False)')
    parser.add_argument('-H', '--hdfs',
                        default = 'hdfs dfs',
                        help = 'hadoop hdfs command (default "hdfs dfs")')
    parser.add_argument('-a', '--age',
                        type = int,
                        default = 7,
                        help = 'age in days (default 7')
    parser.add_argument('-m', '--max',
                        type = int,
                        default = 100,
                        help = 'max files to delete (default 100)')
    parser.add_argument('-b', '--batch',
                        type = int,
                        default = 10,
                        help = 'batch size to delete (default 10)')
    parser.add_argument('-t', '--tmp',
                        default = "/tmp",
                        help = 'tmp dir (default /tmp')
    parser.add_argument('fsimage', metavar='FSIMAGE', nargs='*',
                        help='path to fsimage file')

    args = parser.parse_args()

    debug = args.debug
    dryrun = args.dryrun
    hdfs = args.hdfs
    age = args.age
    tmp = args.tmp
    max_files = args.max
    batch_size = args.batch
    fsimages = args.fsimage
    if len(fsimages) != 1:
        sys.exit('Need fsimage file path')

    # Main code

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    tmp_clean(dryrun, hdfs, age, tmp, fsimages[0],
              max_deletes = max_files,
              batch_size = batch_size)

if __name__ == '__main__':
    main()

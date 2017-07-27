#! /usr/bin/env python3

"""An automated process to zip, validate, and verify VCFs copied to Aspera"""

import os
import shlex
import subprocess
import sys
import time

from collections import deque
import pandas as pd
import yaml


MY_INPUT_YAML_FILE_NAME = 'vcf_batch.yml'

def main():
    config = load_yaml_config()
    run(config)

with open(MY_INPUT_YAML_FILE_NAME) as ymlfile:
    yam = yaml.load(ymlfile)

class BatchInfo:
    pass

BI = BatchInfo()
BI.__dict__.update(yam)
BI.batch = '{0:02d}'.format(BI.batch)
BI.date = str(BI.date)
BI.batch_name = BI.batch_name.format_map(vars(BI))
BI.source_root = BI.source_root.format_map(vars(BI))
BI.stage_dir_prefix = BI.stage_dir_prefix.format_map(vars(BI))

if not os.path.exists(BI.source_root):
    os.mkdir(BI.source_root)
print(BI.batch)
print(BI.date)
print(BI.batch_name)
print(BI.source_root)

if not os.path.exists(BI.staging_root):
    os.makedirs(BI.staging_root)

# have to be in the directory with the excel file
XL = BI.xl_name+'.xlsx'
vcfs = pd.read_csv(XL)
vcfs.head()
snp_paths = vcfs['snp_vcf_path']
indel_paths = vcfs['indel_vcf_path']

# Checking the numbers
NUM_SNP_VCFS = len(snp_paths)
NUM_INDEL_VCFS = len(indel_paths)

if NUM_SNP_VCFS == NUM_INDEL_VCFS:
    print("equal", NUM_SNP_VCFS)
else:
    print("not equal", NUM_SNP_VCFS, "is not", NUM_INDEL_VCFS, file=sys.stderr)

# Running the compression and checksum script

work_list = deque()
for raw_line in sys.stdin:  #the excel file is not the std input...?
    a, b = raw_line.rstrip().split()
    work_list.append((a, b))

workers = []
max_workers = 2
python = 'python3'
script = 'worker.py'

while work_list or workers:
    while work_list and (len(workers) < max_workers):
        work = work_list.popleft()
        a, b = work
        print('starting', a)
        workers.append(
            subprocess.Popen([python, script, a, b],
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        )
    for index, worker in enumerate(workers):
        worker.poll()
        if worker.returncode is not None:
            print('finished', index)
            if worker.returncode:  # nonzero means error
                # Log the error and do whatever.
                pass
            del workers[index]
    time.sleep(1)


def start_work_vcf(python, script, vcf_path, dest_dir_path):
    #Return a Popen object#
    subprocess.Popen(['python',
                      '/hgsc-software/submissions/../../bgzip_md5.py',
                      '-d',
                      snp_dest_dir_path,
                      s_path],
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    proc = subprocess.Popen([python, script, '- d', dest_dir_path, vcf_path],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    return proc
    

""""
def start_work_indel(python, script, indel_path, dest_dir_path):
    #Return a Popen object#
    subprocess.Popen(['python',
                      '/hgsc-software/submissions/../../bgzip_md5.py',
                      '-d',
                      snp_dest_dir_path,
                      s_path],
                     stdout=subprocess.PIPE,
                     stderr=subprocess.PIPE)
    proc = subprocess.Popen([python, script, '- d', dest_dir_path, indel_path],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    return proc
"""

if __name__ == '__main__':
    main()

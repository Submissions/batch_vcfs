#! /usr/bin/env python3

"""Batch process a collection of VCF files (SNPs and indels) that are
listed in an XLSX file which is itself listed in a YAML file."""

import argparse
from collections import deque
import os
import subprocess
import sys
import time

import pandas as pd
import yaml


DEFAULT_MAX_WORKERS = 2

def main():
    args = parse_args()
    config = load_yaml_config(args.config_file)
    if args.max_workers is not None:
        config.max_workers = args.max_workers
    # TODO: what if user sets to < 1?
    run(config)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config_file', help='a YAML file')
    parser.add_argument('-w', '--max_workers', type=int,
                        help='default {}'.format(DEFAULT_MAX_WORKERS))
    args = parser.parse_args()
    return args


def load_yaml_config(config_file):
    with open(config_file) as ymlfile:
        yam = yaml.load(ymlfile)

    class BatchInfo:
        pass

    config = BatchInfo()
    config_dict = config.__dict__
    config_dict.update(yam)
    if not config.xl_name.endswith('.xlsx'):
        config.xl_name = config.xl_name + '.xlsx'
    config.batch = '{0:02d}'.format(config.batch)
    config.date = str(config.date)
    # TODO: Is there a more elegant way?
    config.batch_name = config.batch_name.format_map(config_dict)
    config.source_root = config.source_root.format_map(config_dict)
    config.batch_dest_root = config.batch_dest_root.format_map(config_dict)
    config.snp_dir = config.snp_dir.format_map(config_dict)
    config.indel_dir = config.indel_dir.format_map(config_dict)

    if not os.path.exists(config.source_root):
        os.mkdir(config.source_root)
    # TODO: replace this kind of printing by logging.
    print(config.batch)
    print(config.date)
    print(config.batch_name)
    print(config.source_root)
    return config


def run(config):
    for dir_path in (config.snp_dir, config.indel_dir):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    # TODO: have to be in the directory with the excel file
    print(repr(config.xl_name))
    vcfs = pd.read_excel(config.xl_name)
    vcfs.head()
    snp_paths = vcfs['snp_path']
    indel_paths = vcfs['indel_path']

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
    max_workers = getattr(config, 'max_workers', DEFAULT_MAX_WORKERS)
    # TODO: Generalize the python3 + worker script concept.
    python = 'python3'
    script = 'bgzip_md5_v2.py'

    while work_list or workers:
        while work_list and (len(workers) < max_workers):
            work = work_list.popleft()
            a, b = work
            print('starting', a)
            # TODO: Unify all thi Popen stuff.
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
                      'script',
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

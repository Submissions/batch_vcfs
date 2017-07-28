#! /usr/bin/env python3

"""Batch process a collection of VCF files (SNPs and indels) that are
listed in an XLSX file which is itself listed in a YAML file.

Sample YAML:

batch: 1
date: 2017-06-16
worklist_file: test.tsv
batch_dest_root: '/tmp/testing/aspera/share/submissions/staging/test/one/{batch_name}'
batch_name: VTE_Mayo_test{batch}_{date}
source_root: '{batch_name}'
snp_dir: '{batch_dest_root}/{batch_name}_SNPs'
indel_dir: '{batch_dest_root}/{batch_name}_indels'
"""

import argparse
import os
from pprint import pprint  # TODO: replace by logging later
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
    pprint(config.__dict__)
    return config


def run(config):
    for dir_path in (config.snp_dir, config.indel_dir):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    # TODO: have to be in the directory with the excel file
    print(repr(config.worklist_file))
    if config.worklist_file.endswith('.xlsx'):
        vcfs = pd.read_excel(config.worklist_file)
    else:
        vcfs = pd.read_table(config.worklist_file)
    vcfs.rename(  # The input names are not always consistent.
        columns={'indel_vcf_path': 'indel_path', 'snp_vcf_path': 'snp_path'}
    )
    snp_paths = vcfs.snp_path
    indel_paths = vcfs.indel_path

    # Checking the numbers
    NUM_SNP_VCFS = len(snp_paths)
    NUM_INDEL_VCFS = len(indel_paths)

    if NUM_SNP_VCFS == NUM_INDEL_VCFS:
        print("equal", NUM_SNP_VCFS)
    else:
        print("not equal", NUM_SNP_VCFS, "is not", NUM_INDEL_VCFS, file=sys.stderr)

    worklist = []  # Pairs of (input file & destination dir)
    for snp_path in snp_paths:
        worklist.append((snp_path, config.snp_dir))
    for indel_path in indel_paths:
        worklist.append((indel_path, config.indel_dir))
    pprint(worklist)

    max_workers = getattr(config, 'max_workers', DEFAULT_MAX_WORKERS)
    # Running the compression and checksum script
    handle_backlog_with_workers(worklist, config.program_args, max_workers)


def handle_backlog_with_workers(worklist, fixed_args, max_workers):
    """Run a loop with a maximum number of subprocesses. Consumes"""
    backlog = worklist[::-1]  # Using pop() will iterate in original order.
    workers = []
    while backlog or workers:
        while backlog and (len(workers) < max_workers):
            unit_of_work = backlog.pop()
            vcf_path, dest_dir_path = unit_of_work
            print('starting', vcf_path)
            worker = Worker(fixed_args, vcf_path, dest_dir_path)
            print('started', worker.vcf_name)
            workers.append(worker)
        for index, worker in enumerate(workers):
            worker.proc.poll()
            if worker.proc.returncode is not None:
                print('finished', worker.vcf_name)
                if worker.proc.returncode:  # nonzero means error
                    # Log the error and do whatever.
                    print('error', worker.vcf_name)
                del workers[index]
        time.sleep(1)


class Worker:
    """Composite of a subprocess (proc) and an open file (file). The stderr
    and stdout are combined into that single output file."""
    def __init__(self, fixed_args, vcf_path, dest_dir_path):
        os.makedirs(dest_dir_path, exist_ok=True)
        args = fixed_args + [dest_dir_path, vcf_path]
        vcf_name = os.path.basename(vcf_path)
        log_name = vcf_name + '.log'
        log_path = os.path.join(dest_dir_path, log_name)
        self.vcf_name = vcf_name
        self.file = open(log_path, 'wb')
        self.proc = subprocess.Popen(args,
                                     stdout=self.file,
                                     stderr=subprocess.STDOUT)


if __name__ == '__main__':
    main()

# batch_vcfs

This tool will

1. Read the YAML file designated on the command line.
2. Open the XLSX file named in the YAML file.
3. Run a worker script for each SNP and indel VCF file whose path is specified in the XLSX file.

The worker script instances will be run in parallel as child processes with a maximum number of simultaneous instances.

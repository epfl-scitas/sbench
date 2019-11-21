#!/bin/bash -l

scratch_dir="/scratch/${USER}"

# Create a virtual environment in a random location
mkdir -p ${scratch_dir}/sbench/venv
venv_dir="$(mktemp -d ${scratch_dir}/sbench/venv/py-36.XXXX)"

module load gcc python
virtualenv --version
virtualenv --python=python3.6 ${venv_dir}

# Install sbench in the virtual environment
. ${venv_dir}/bin/activate
pip install -e .

# Set the targets of this run
hostname="$(hostname)"

if [ "${hostname}" = fidis ] ;
then
    target_flag="--clusters=fidis,gacrux"
elif [ "${hostname}" = helvetios ] ;
then 
    target_flag="--clusters=helvetios"
elif [[ "${hostname}" =~ deneb[12] ]] ;
then
    target_flag="--clusters=deneb,eltanin"
else
    echo "Unknown hostname ${hostname}."
    exit 1
fi

# Run the benchmarks
benchmarks_dir="${scratch_dir}/sbench/benchmarks/${hostname}"
mkdir -p ${benchmarks_dir}

echo SUBMITTING JOBS [$hostname]
sbench run ${target_flag} ${benchmarks_dir}

# Update the DB
db_dir="${HOME}/benchmarks/db"
raw_results_dir="${HOME}/benchmarks/raw"
mkdir -p ${db_dir} && mkdir -p ${raw_results_dir}


function join_by { local IFS="$1"; shift; echo "$*"; }

job_names="hpl,osu_bw,osu_bibw,osu_latency,osu_alltoall,osu_allreduce"
job_deps=$(join_by ':' $(squeue --name=${job_names} --format="%.i" | tail -n +2))

echo WAITING JOBS [$hostname]

# Here we are touching a file that will be used as a
# semaphore to indicate that we still need to wait for
# some job to finish. This file will be deleted as the
# final step of the process spawned in the background.
# This is done because otherwise Jenkins (Java) will
# timeout if the process is not producing any output
# for 5 mins.
touch ${benchmarks_dir}/waiting

# Spawn the background process
echo srun --dependency=afterany:${job_deps} -- sbench collect --db ${db_dir}/benchmarks.db ${benchmarks_dir}
srun --dependency=afterany:${job_deps} -- sbench collect --db ${db_dir}/benchmarks.db ${benchmarks_dir} && rm ${benchmarks_dir}/waiting &

# Wait for the background process to finish
while [ -f ${benchmarks_dir}/waiting ] ;
do
    printf . && sleep 5
done
echo

# Archive all the raw data
echo ARCHIVING DATA [${hostname}]
tar -czvf ${raw_results_dir}/benchmarks-${hostname}-$(date --rfc-3339=date).tgz --remove-files ${benchmarks_dir}/*

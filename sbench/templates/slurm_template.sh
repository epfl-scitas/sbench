#!/bin/bash -l

#SBATCH --account=scitas-ge
#SBATCH --qos=scitas
#SBATCH --job-name {{ name }}
{% if target %}
#SBATCH --constraint={{ target }}
{% endif %}
#SBATCH --mem=MaxMemPerNode
#SBATCH --exclusive
#SBATCH --nodes={{ nnodes }}
{% if ntasks %}
#SBATCH --ntasks={{ ntasks }}
{% endif %}
#SBATCH --cpus-per-task=1
#SBATCH --output={{ output_file }}
#SBATCH --error={{ error_file }}
{% for directive in extra_directives %}
{{ directive }}
{% endfor %}

# Print the date (gives a time reference to the parser)
date -R >> {{ test_directory }}/run.${SLURM_JOB_ID}.start

env >> {{ test_directory }}/run.${SLURM_JOB_ID}.env

# Execute the benchmark
module load {{ compiler }} {{ mpi }}

{% include test_template %}

date -R >> {{ test_directory }}/run.${SLURM_JOB_ID}.finished

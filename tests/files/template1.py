#!/bin/bash
#SBATCH --job-name={{ jobname }}
#SBATCH --output={{ output_file }}
#SBATCH --account={{ account }}
#SBATCH --time={{ time }}
#SBATCH --nodes={{ nnodes }}
#SBATCH --ntasks={{ ntasks }}
#SBATCH --cpus-per-task={{ cpus_per_task }}
#SBATCH --partition={{ partition }}
{{ module_loads }}

srun pr -N {{ nnodes }} -n {{ ntasks }} $BIN -a {{ api }} -t {{ xfer_size }} -b {{ block_size }} -s {{ segment_size }}

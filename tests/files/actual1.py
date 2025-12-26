#!/bin/bash
#SBATCH --job-name=MLR2D
#SBATCH --output=reports/submit1_fpp_project_%j.out
#SBATCH --account=cstao
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --cpus-per-task=1
#SBATCH --partition=xxx

ml purge
ml GCC OpenMPI
ml git


srun pr -N 1 -n 16 $BIN -a mpiio -t 16m -b 2g -s 1

#!/bin/bash
#SBATCH --verbose
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
#SBATCH --job-name=kabhi
#SBATCH --mem=100G
#SBATCH --partition=courses
$SRUN mpirun -mca btl_bas:wq:quUie_warn_component_unused 0 output 128

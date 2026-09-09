#!/bin/bash
#SBATCH --job-name=wandb-slurm-meta
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --gpus-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:20:00
#SBATCH --output=%x-%j.out

# wandb.ai credentials + target project (WANDB_BASE_URL / API_KEY / ENTITY / PROJECT)
source ~/.wandb_saas_env
export WANDB_DIR=$HOME/wandb_runs
mkdir -p "$WANDB_DIR"

MINUTES=${1:-13}
srun /opt/conda/bin/python3 "$HOME/slurm_test/burn.py" "$MINUTES"

"""Per-node W&B run that keeps one GPU busy for N minutes.

Used to produce runs with Slurm metadata (wandb-metadata.json -> slurm) for the
gpu_dashboard Slurm dedupe test. One run per node, same SLURM_JOB_ID.
"""
import os
import sys
import time

import wandb

minutes = float(sys.argv[1]) if len(sys.argv) > 1 else 13.0
job_id = os.environ.get("SLURM_JOB_ID", "nojob")
node_id = os.environ.get("SLURM_NODEID", "0")

run = wandb.init(
    name=f"job{job_id}-node{node_id}",
    group=f"slurm-{job_id}",
    config={"minutes": minutes, "slurm_job_id": job_id, "nodeid": node_id},
)

try:
    import torch
    dev = torch.device("cuda")
    a = torch.randn(8192, 8192, device=dev)
    b = torch.randn(8192, 8192, device=dev)
except Exception as e:  # torch missing or no CUDA: still produce a run, just no GPU load
    print(f"torch/cuda unavailable ({e}); idling instead", flush=True)
    torch = None

t0 = time.time()
step = 0
while time.time() - t0 < minutes * 60:
    if torch is not None:
        for _ in range(20):
            a = (a @ b).tanh()
        torch.cuda.synchronize()
        val = float(a[0, 0])
    else:
        time.sleep(5)
        val = 0.0
    step += 1
    run.log({"loss": 1.0 / (step + 1), "probe": val, "elapsed_min": (time.time() - t0) / 60})

run.finish()

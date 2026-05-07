# Scalable Histogram Computation using MPI

Parallel histogram computation in **C** and **OpenMPI** for distributed-memory systems. The program partitions a large random dataset across MPI processes, computes local histograms independently, and aggregates the final histogram with `MPI_Reduce`.

The benchmark results show the main distributed-systems tradeoff: adding more MPI processes can increase communication and synchronization overhead faster than it reduces local computation time.

## Recruiter Snapshot

| Area | Summary |
| --- | --- |
| Problem | Compute histograms over randomly generated values in the range `1` to `100,000`. |
| Scale tested | `800,000` generated entries in the included result logs. |
| Parallelism | Up to `8` nodes, `16` tasks per node, and `128` total MPI processes. |
| Core MPI operations | `MPI_Send` for partition transfer and `MPI_Reduce` for final aggregation. |
| Result focus | Benchmarked 32-bin vs 128-bin histograms and shared-memory vs distributed-memory execution. |
| Key finding | Communication overhead dominates as work spreads across more nodes, especially when each process has relatively light local work. |

## Results at a Glance

### 128-Bin Histogram

| Nodes | Tasks per Node | Total MPI Processes | Time Consumed (s) |
| ---: | ---: | ---: | ---: |
| 2 | 16 | 32 | 0.197938 |
| 4 | 16 | 64 | 0.508792 |
| 8 | 16 | 128 | 1.124566 |

Increasing from 32 to 128 MPI processes increased total runtime because inter-process communication, synchronization, and reduction overhead outweighed the benefit of smaller local partitions.

### 32-Bin Histogram

| Nodes | Tasks per Node | Total MPI Processes | Time Consumed (s) |
| ---: | ---: | ---: | ---: |
| 2 | 16 | 32 | 0.173933 |
| 4 | 16 | 64 | 0.488193 |

The 32-bin runs were faster than the 128-bin runs at the same process count because each local histogram had fewer bins to compare, store, and reduce.

### Shared Memory vs Distributed Memory

| Setup | Nodes | Tasks per Node | Total MPI Processes | Time Consumed (s) |
| --- | ---: | ---: | ---: | ---: |
| Shared-memory intensive | 1 | 8 | 8 | 0.151507 |
| Communication intensive | 8 | 1 | 8 | 0.224762 |

With the same total number of MPI processes, the 8-node distributed-memory run was about 48% slower than the 1-node shared-memory run. This highlights the cost of network communication and cross-node reduction even when compute parallelism is held constant.

## How It Works

1. Rank 0 generates the random input dataset.
2. The dataset is partitioned across MPI processes.
3. Each process computes a local histogram for its assigned data chunk.
4. Local histograms are combined on the root process using `MPI_Reduce`.
5. The root process prints the final histogram, total count validation, bin count, process count, and runtime.

## Repository Layout

```text
.
|-- mpi_histogram.c
|-- run_histogram.slurm
|-- MPI_Histogram_Report.pdf
|-- Histogram_128_Bin_Results/
|-- Histogram_32_Bin_Results/
`-- Performance_Experiments/
```

The result folders contain the raw benchmark logs used in the tables above.

## Build and Run

Compile the MPI program:

```bash
mpicc -o output mpi_histogram.c
```

Submit through Slurm:

```bash
sbatch run_histogram.slurm
```

The Slurm script controls the node count, tasks per node, and histogram bin count:

```bash
$SRUN mpirun -mca btl_bas:wq:quUie_warn_component_unused 0 output 128
```

Change the final argument to switch bin counts, for example `32` or `128`.

## Report

See [`MPI_Histogram_Report.pdf`](MPI_Histogram_Report.pdf) for the full experiment walkthrough, output screenshots, and performance discussion.

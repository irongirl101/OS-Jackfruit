# Documentation

# OS-Jackfruit: Multi-Container Runtime

A lightweight Linux container runtime in C with a long-running supervisor and a kernel-space memory monitor.

## 1. Team Information
- ** Aditi Vignesh - PES1UG24CS023
- ** Piya Banerjee - PES1UG25CS830

## 2. Build, Load Instructions 
```
# Build the Project
make

# Build the Supervisor - engine 
gcc -o engine engine.c -lpthread

# Load the Kernel Module
sudo insmod monitor.ko
# check if module is loaded 
lsmod | grep monitor
# check kernel logs
dsmeg | tail

# Start the supervisor
sudo ./engine supervisor ./rootfs-base

# Create per-container writable rootfs copies
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta

# list Running containers
sudo ./engine ps

# inspect a container
sudo ./engine logs alpha

# CPU Workload
./cpu_hog

# Memory workload
./memory_hog

# I/O Workload
./io_pulse

# Monitoring kernel logs
dmesg | tail -f

# stop all workloads
killall cpu_hog memory_hog io_pulse

# unload kernel module
sudo rmmod monitor
lsmod | grep monitor # verify removal

# clean build files
make clean

# Stop containers
sudo ./engine stop alpha
sudo ./engine stop beta
```
## 3. Demo with Screenshots

### 1. Multi-Container Supervision
![Multi-container supervision](images/image-1.png)
Multiple containers (alpha, beta) running under a single supervisor process.

### 2. Metadata Tracking

![Metadata tracking](images/image-2.png)
Supervisor metadata tracking for active containers.

### 3. Bounded-Buffer Logging
![Bounded-buffer logging](images/image-3.png)
Bounded-buffer logging pipeline showing producer-consumer behavior with real-time container logs.

### 4. CLI and IPC
![CLI and IPC](images/image-4.png)
CLI command interacting with supervisor via IPC mechanism (user-space communication).

### 5. Soft-Limit Warning
![Soft-limit warning](images/image-5.png)
Kernel module reporting soft memory limit warning for container gamma.

### 6. Hard-Limit Enforcement
![Hard-limit enforcement](images/image-6.png)
Hard memory limit enforced - container terminated by kernel and reflected in supervisor metadata.

### 7. Scheduling Experiment
![Scheduling experiment](images/image-7.png)
Scheduling experiment showing CPU and I/O workload interaction and observable resource sharing differences.

### 8. Clean Teardown
![Clean teardown](images/image-8.png)
Clean teardown with all containers reaped and no zombie processes remaining.


## 4. Engineering Analysis

### 4.1 Isolation Mechanisms
Our runtime achieves process isolation through Linux namespaces created at container spawn time using `clone()` with `CLONE_NEWPID`, `CLONE_NEWUTS`, and `CLONE_NEWNS` flags. 
The PID namespace gives each container its own process numbering where the init process sees itself as PID 1, while the host maintains the real PIDs. 
The UTS namespace provides the container with its own hostname, independent from the host system. 
The mount namespace is critical because it gives each container a private mount table, preventing the container from seeing or modifying host mounts. 
Combined with `chroot`, which remaps the container's root filesystem to its assigned rootfs directory, this creates filesystem isolation. 
What the host kernel still shares with all containers is the system call interface, time, and resource limits because these namespaces were not created. 
We also do not isolate the network or user namespaces, meaning containers share the host's network interfaces and UID mappings.

### 4.2 Supervisor and Process Lifecycle
A long-running parent supervisor is very important because it provides a parent process that exists beyond individual container lifetimes.
Without it, each container would be an orphaned process, and we would lose the ability to track container metadata after they exit.
When we use `clone()` to create container processes, the supervisor keeps the parent-child relationship, allowing proper signal delivery and exit status through `waitpid()`.
This matters for correct reaping to prevent zombie processes and for tracking why each container exited. 
The supervisor also distinguishes between a normal exit, a graceful stop initiated by the `stop` command, and a hard-limit kill by the kernel module, by setting a `stop_requested` flag before sending signals.
This flag is checked during child reap to classify the termination reason in metadata.

### 4.3 IPC, Threads, and Synchronization
Our runtime uses two separate IPC paths.
Path A is the logging pipeline where each container's `stdout` and `stderr` flow through pipes to producer threads, into a bounded buffer, and out to consumer threads that write log files.
The bounded buffer is a circular queue protected by a mutex with two condition variables for signaling when the buffer is not empty or not full, allowing producers to block when the buffer fills rather than dropping data. 
Path B is the control channel using a UNIX domain socket where CLI commands are sent to a supervisor handler thread that processes requests and sends responses over connected sockets.
For metadata access, we use a single mutex protecting the container linked list because metadata operations are infrequent and relatively short-lived, so the contention is low. 
The kernel module uses a mutex for the monitored process list since the timer callback runs only once per second, making the latency overhead of a mutex acceptable compared to a spinlock that would burn CPU cycles while waiting.

### 4.4 Memory Management and Enforcement
Resident Set Size (RSS) measures the number of memory pages currently held in physical RAM. 
It does not measure pages swapped to disk, memory-mapped file contents that are not resident, or cache pages that could be reclaimed. 
The soft and hard limits serve different policies because a soft limit acts as an early warning mechanism that logs a single event when first exceeded, while a hard limit enforces termination to prevent a runaway process from consuming available memory. 
The enforcement mechanism belongs in kernel space rather than user space because only the kernel has direct access to `struct mm_struct` containing accurate RSS information, and user space cannot reliably intercept memory allocations or page faults to enforce limits, and any user-space polling would have inherent race conditions with rapid allocations.

### 4.5 Scheduling Behaviour
When running two CPU-bound workloads with different nice values, the scheduler heavily favored the high-priority task (nice -20), allowing it to complete significantly faster than the low-priority task (nice 19). 
This shows that while CFS aims for fairness; it uses virtual runtime (`vruntime`) weighting to allow users to deliberately sacrifice the throughput of lower-priority processes to ensure critical tasks get CPU time.
Furthermore, when running a CPU-bound workload alongside an I/O-bound workload at the same priority, the I/O-bound task remained highly responsive and completed its iterations first. 
Because the I/O-bound task spends most of its time sleeping or waiting on I/O, its `vruntime` grows very slowly compared to the CPU-bound task which never voluntarily yields the CPU. 
When the I/O-bound task wakes up, its lower `vruntime` causes CFS to immediately preempt the CPU-bound task. 
This behavior highlights how Linux scheduling achieves low latency and high responsiveness for interactive tasks without completely starving CPU-bound tasks, dynamically optimizing overall system fairness and throughput.
## 5. Design Decisions and Tradeoffs

### Namespace Isolation
We used:
- CLONE_NEWPID (creates a new PID namespace with child getting PID 1)
- CLONE_NEWUTS (creates a new UTS namespace gives the container its own hostname)
- CLONE_NEWNS (creates a new mount namespace, giving the container its own mount table) 
- chroot (which changes the root filesystem, preventing the container from accessing files outside its rootfs) 
to provide the namespace isolation with the containers. 
The tradeoff made is that this doesn't provide network namespace, user namespace (shares the same UIDs), IPC namespace (allows for shared memory between containers), and device isolation (/dev from host).
We didn't implement network namespace isolation because the container will have to manage the interfaces.

### Supervisor Architecture
The supervisor architecture has a long-running parent process with per-command client spawned.
The supervisor is the parent process for all the containers. This architecture enables shared logging pipeline and metadata tracking across containers, and increases reliability when containers fail.

### IPC/Logging
For IPC and logging, we use a UNIX domain socket for control, and use pipes and a mutex for logging. 
The tradeoff made with the UNIX socket is that we have to cross the userspace-kernel boundary twice when sending and receiving commands. 
This is acceptable here, because sending and recieving commands is an infrequent operation, and using UNIX sockets prevent having to synchronise this operation.

### Kernel Monitor
The kernel monitor is a kernel module with linked list and timer-based polling, where we use a mutex over a spinlock for acccessing the container list.
The tradeoff made here is latency, a mutex would cause there to be another context-switch when the lock is held.
This is acceptable because the critical section is small and the timer only runs once every second, which is minimal.
We also prefer a mutex over spinlock because it reduces the amount of CPU cycles burned waiting for the lock to be released.

## 6. Scheduler Experiment Results

To demonstrate the behavior of the Linux scheduler, two controlled scheduling experiments were conducted using our multi-container runtime. The experiments used two distinct workloads: `cpu_hog` (a purely CPU-bound process that continuously burns cycles) and `io_pulse` (an I/O-bound process that performs short bursts of I/O followed by sleeping). 

### Experiment A: Nice Value Comparison (CPU vs. CPU)
In this experiment, two CPU-bound containers were launched simultaneously but with drastically different priorities to observe CPU allocation.
- **Container 1 (`high_prio`):** `cpu_hog` running at `nice = -20` (Highest priority)
- **Container 2 (`low_prio`):** `cpu_hog` running at `nice = 19` (Lowest priority)

**Observations:**
During execution, the `high_prio` container used most of the CPU cycles. While both workloads were tasked with completing 15 iterations, `high_prio` progressed rapidly and finished its execution long before `low_prio` made significant progress. The CPU share allocation heavily favored the `high_prio` container, while the `low_prio` container was heavily penalised and starved of CPU time. Consequently, `high_prio` completed first (fastest) and `low_prio` completed second.

**Analysis:**
This outcome illustrates how the Linux Completely Fair Scheduler (CFS) utilizes `nice` values to weight the virtual runtime (`vruntime`) of processes. Because a `nice` value of -20 has a drastically higher weight than 19, CFS allocates a significantly larger time slice to the `high_prio` container. Meanwhile, the `vruntime` of the `low_prio` container advances much faster relative to its actual execution time, causing it to quickly lose its turn on the CPU. This demonstrates how priority weighting can be used to explicitly favor critical CPU tasks.

### Experiment B: CPU-Bound vs. I/O-Bound
In this experiment, a CPU-bound container and an I/O-bound container were launched simultaneously with identical priorities (`nice = 0`).
- **Container 1 (`cpu_work`):** `cpu_hog` (CPU-bound)
- **Container 2 (`io_work`):** `io_pulse` (I/O-bound)

**Observations:**
Despite the `cpu_work` container constantly demanding CPU cycles, the `io_work` container remained highly responsive and progressed steadily. The `cpu_work` container experienced continuous execution of its pure CPU computation workload, while the `io_work` container experienced immediate scheduling upon waking for its intermittent I/O + sleep workload. The I/O-bound workload completed its 20 iterations efficiently and finished before the CPU-bound workload, even though they shared the same base priority. Therefore, `io_work` completed first and `cpu_work` completed second.

**Analysis:**
This experiment highlights CFS's inherent bias toward interactive and I/O-bound tasks to maintain system responsiveness. Because `io_pulse` spends most of its time sleeping or waiting on I/O, its `vruntime` grows very slowly compared to the `cpu_hog`, which never voluntarily yields the CPU. 

When the I/O-bound task wakes up, its `vruntime` is significantly lower than that of the CPU-bound task. Consequently, CFS immediately preempts the CPU task to allow the I/O task to run. Once the brief I/O burst is over and the task blocks again, the CPU task resumes. This design allows I/O-bound processes to remain highly responsive and complete their tasks without being starved by CPU-heavy neighbors.



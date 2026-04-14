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





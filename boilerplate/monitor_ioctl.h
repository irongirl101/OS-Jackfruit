set -e

RUNTIME="sudo ./engine"
ROOTFS_ALPHA=rootfs-alpha
ROOTFS_BETA=rootfs-beta

mkdir -p logs

echo "Starting supervisor..."
$RUNTIME supervisor rootfs-base &
SUP_PID=$!
sleep 1

echo ""
echo "=== Experiment A: Nice Value Comparison ==="
echo "Running two cpu_hog instances: one with nice=-20, one with nice=19"
$RUNTIME run high_prio $ROOTFS_ALPHA "/bin/cpu_hog 15" --nice -20 &
$RUNTIME run low_prio $ROOTFS_BETA "/bin/cpu_hog 15" --nice 19 &

echo ""
echo "Polling ps every 3 seconds..."
for i in 1 2 3 4 5 6; do
    echo "--- Snapshot $i (after $((i * 3))s) ---"
    $RUNTIME ps
    sleep 3
done

echo ""
echo "=== Experiment B: CPU-bound vs I/O-bound ==="
echo "Running cpu_hog and io_pulse at same nice=0"
$RUNTIME run cpu_work $ROOTFS_ALPHA "/bin/cpu_hog 20" --nice 0 &
$RUNTIME run io_work $ROOTFS_BETA "/bin/io_pulse 20 200" --nice 0 &

echo ""
echo "Polling ps every 2 seconds..."
for i in 1 2 3 4 5 6 7 8 9 10; do
    echo "--- Snapshot $i (after $((i * 2))s) ---"
    $RUNTIME ps
    sleep 2
done

echo ""
echo "Collecting logs..."
mkdir -p exp-logs
$RUNTIME logs high_prio >exp-logs/experiment_a_high.log 2>&1 || true
$RUNTIME logs low_prio >exp-logs/experiment_a_low.log 2>&1 || true
$RUNTIME logs cpu_work >exp-logs/experiment_b_cpu.log 2>&1 || true
$RUNTIME logs io_work >exp-logs/experiment_b_io.log 2>&1 || true

sleep 1
kill $SUP_PID 2>/dev/null || true
wait $SUP_PID 2>/dev/null || true

echo ""
echo "Experiment complete. Logs saved to exp-logs/"

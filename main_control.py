# ==============================================================
# main_controller.py
# ==============================================================
# Purpose:
#   - Start publisher (Task 1–3) and subscriber (Task 4) concurrently
#   - Let them run together for testing or full streaming simulation
# ==============================================================

import subprocess
import time
import signal
import sys

# ==============================================================
# CONFIG
# ==============================================================
PUBLISHER_SCRIPT  = "task1_to_3_pipeline.py"
SUBSCRIBER_SCRIPT = "task4_pipeline.py"
TEST_ROUNDS = 3            # for testing; your publisher already has a 3-round loop
SLEEP_BETWEEN_ROUNDS = 60  # seconds per round (same as in publisher)
ESTIMATED_TIME = TEST_ROUNDS * (SLEEP_BETWEEN_ROUNDS + 10)  # rough total seconds

# ==============================================================
# MAIN CONTROL LOGIC
# ==============================================================
def main():
    print("[MAIN] Launching subscriber first...")
    sub_proc = subprocess.Popen(["python", SUBSCRIBER_SCRIPT])
    time.sleep(3)  # let subscriber connect to broker before publishing

    print("[MAIN] Launching publisher...")
    pub_proc = subprocess.Popen(["python", PUBLISHER_SCRIPT])

    try:
        print(f"[MAIN] Running both processes for about {ESTIMATED_TIME} seconds...\n")
        time.sleep(ESTIMATED_TIME)
    except KeyboardInterrupt:
        print("[MAIN] Interrupted manually. Stopping both processes...")

    # ==========================================================
    # Graceful shutdown
    # ==========================================================
    for name, proc in [("Publisher", pub_proc), ("Subscriber", sub_proc)]:
        if proc.poll() is None:  # still running
            print(f"[MAIN] Terminating {name}...")
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"[MAIN] Forcing {name} to stop...")
                proc.kill()

    print("[MAIN] All processes stopped.")
    sys.exit(0)


if __name__ == "__main__":
    main()


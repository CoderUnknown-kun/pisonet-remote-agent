import os
import time
import subprocess
import sys
import ctypes
import psutil

APP_DIR = os.path.dirname(os.path.abspath(sys.argv[0]))

CURRENT_EXE = os.path.join(APP_DIR, "pisonet_agent.exe")
NEW_EXE = os.path.join(APP_DIR, "pisonet_agent_new.exe")
BACKUP_EXE = os.path.join(APP_DIR, "pisonet_agent.exe.old")

LOG_FILE = os.path.join(APP_DIR, "updater.log")


def log(msg):
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except:
        pass


def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False


def relaunch_as_admin():
    log("Requesting admin privileges")

    ctypes.windll.shell32.ShellExecuteW(
        None,
        "runas",
        sys.executable,
        f'"{os.path.abspath(sys.argv[0])}"',
        None,
        1
    )

    sys.exit()



def wait_for_process_exit(process_name="pisonet_agent.exe", timeout=30):

    log("Waiting for agent process to exit...")

    start = time.time()

    while time.time() - start < timeout:

        running = False

        for proc in psutil.process_iter(["name"]):
            try:
                name = proc.info["name"]
                if name and name.lower() == process_name.lower():
                    running = True
                    break
            except:
                pass

        if not running:
            log("Agent process exited")
            return True

        time.sleep(0.5)

    log("Agent still running after timeout")
    return False

def safe_replace():

    log("Installing update")

    for attempt in range(20):

        try:

            if os.path.exists(BACKUP_EXE):
                os.remove(BACKUP_EXE)

            if os.path.exists(CURRENT_EXE):
                os.rename(CURRENT_EXE, BACKUP_EXE)

            os.rename(NEW_EXE, CURRENT_EXE)
            time.sleep(0.5)
            log("Replace successful")

            return True

        except PermissionError:

            log("File locked, retrying...")
            time.sleep(1)
    # attempt rollback
    if os.path.exists(BACKUP_EXE) and not os.path.exists(CURRENT_EXE):
        try:
            os.rename(BACKUP_EXE, CURRENT_EXE)
            log("Rollback successful")
        except:
            log("Rollback failed")

    return False


def restart_agent():

    log("Restarting agent")

    subprocess.Popen(
        [CURRENT_EXE],
        cwd=APP_DIR,
        creationflags=subprocess.CREATE_NO_WINDOW
    )


def main():

    log("Updater started")

    if not is_admin():
        relaunch_as_admin()

    time.sleep(2)

    if not os.path.exists(NEW_EXE):
        log("No new exe found")
        return

    if not wait_for_process_exit():
        log("Agent still locked. Abort.")
        return

    if not safe_replace():
        log("Replace failed")
        return

    restart_agent()

    log("Update completed")


if __name__ == "__main__":
    main()

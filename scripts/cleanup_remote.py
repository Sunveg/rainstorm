#!/usr/bin/env python3
import asyncio
import iterm2
import subprocess

# ------------ CONFIG ------------
USER = "root"
CLUSTER_PREFIX = "fa25-cs425-10"   # -> fa25-cs425-1001..1010
DOMAIN = "cs.illinois.edu"
REMOTE_WORKDIR = "/root/MP/MP3"
SSH_SETTLE = 1.2                   # seconds after ssh before sending commands
# --------------------------------

def host_label(i: int) -> str:
    return f"{CLUSTER_PREFIX}{i:02d}"

def host_fqdn(i: int) -> str:
    return f"{host_label(i)}.{DOMAIN}"

async def ssh_only(session: iterm2.Session, host: str):
    await session.async_send_text(f"ssh {USER}@{host}\n")
    await asyncio.sleep(SSH_SETTLE)

async def send_cmd(session: iterm2.Session, cmd: str):
    await session.async_send_text(cmd + "\n")

async def main(connection):
    app = await iterm2.async_get_app(connection)
    window = app.current_terminal_window
    if window is None:
        window = await app.async_create_window()
    tab = window.current_tab
    session0 = tab.current_session

    # Build a 2×5 grid
    top_row = [session0]
    cur = session0
    for _ in range(4):  # 5 columns total
        right = await cur.async_split_pane(vertical=True)
        top_row.append(right)
        cur = right

    bottom_row = []
    for s in top_row:
        b = await s.async_split_pane(vertical=False)  # create second row
        bottom_row.append(b)

    node_indices_top = list(range(1, 6))      # 1001..1005
    node_indices_bottom = list(range(6, 11)) # 1006..1010

    # === Phase 1: SSH into ALL panes immediately (in parallel) ===
    ssh_tasks = []
    
    # top row
    for pane, idx in zip(top_row, node_indices_top):
        ssh_tasks.append(ssh_only(pane, host_fqdn(idx)))
    
    # bottom row
    for pane, idx in zip(bottom_row, node_indices_bottom):
        ssh_tasks.append(ssh_only(pane, host_fqdn(idx)))
    
    await asyncio.gather(*ssh_tasks)

    # === Phase 2: CD into workdir in all panes (parallel) ===
    cd_tasks = []
    for pane in top_row + bottom_row:
        cd_tasks.append(send_cmd(pane, f"cd {REMOTE_WORKDIR}"))
    
    await asyncio.gather(*cd_tasks)
    
    # Equalize panes
    try:
        subprocess.run(
            [
                "osascript",
                "-e",
                'tell application "System Events" to tell process "iTerm2" to '
                'click menu item "Split Panes Evenly" of menu "Arrange Panes" of '
                'menu item "Arrange Panes" of menu "View" of menu bar 1'
            ],
            check=True
        )
    except Exception as e:
        print("Could not equalize panes automatically:", e)

iterm2.run_until_complete(main)


#!/usr/bin/env python3
import asyncio
import iterm2
import subprocess

# ------------ CONFIG ------------
USER = "root"
CLUSTER_PREFIX = "fa25-cs425-10"   # -> fa25-cs425-1001..1010
DOMAIN = "cs.illinois.edu"
REMOTE_WORKDIR = "/root/MP/MP3"      # Adjust if your remote path is different
DAEMON_BIN = "./hydfs"
BASE_IDX = 1                       # 1001 is the base
BASE_PORT = 8000 + BASE_IDX        # 8001
NUM_NODES = 10                     # 1001..1010
SSH_SETTLE = 1.2                   # seconds after ssh before sending commands
# --------------------------------

def host_label(i: int) -> str:
    return f"{CLUSTER_PREFIX}{i:02d}"

def host_fqdn(i: int) -> str:
    return f"{host_label(i)}.{DOMAIN}"

def port_for(i: int) -> int:
    return 8000 + i

def base_daemon_cmd():
    # Run hydfs in foreground (exec) so pane stdin goes to it
    return (
        f'ME_IP="$(hostname -I | awk \'{{print $1}}\')" '
        f'&& cd "{REMOTE_WORKDIR}" '
        f'&& exec {DAEMON_BIN} "$ME_IP" {port_for(BASE_IDX)}'
    )

def child_daemon_cmd(i: int):
    port = port_for(i)
    # Resolve base IP; multiple fallbacks
    return (
        'ME_IP="$(hostname -I | awk \'{print $1}\')" '
        '&& BASE_IP="$(getent hosts fa25-cs425-1001 | awk \'{print $1}\')" '
        '|| BASE_IP="$(dig +short fa25-cs425-1001 | head -n1)" '
        '|| BASE_IP="$(host fa25-cs425-1001 | awk \'/has address/{print $4; exit}\')" '
        f'&& cd "{REMOTE_WORKDIR}" '
        f'&& exec {DAEMON_BIN} "$ME_IP" {port} "$BASE_IP:{port_for(BASE_IDX)}"'
    )

def list_mem_cmd():
    return "membership"

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

    # === Phase 2: Orchestrate daemon starts and probes ===
    
    # 2a) Start BASE (1001) only
    await send_cmd(top_row[0], base_daemon_cmd())
    
    # small pause; then list_mem on base (stdin attached to daemon)
    await asyncio.sleep(2)
    await send_cmd(top_row[0], "\n\n")
    await send_cmd(top_row[0], list_mem_cmd())
    
    # 2b) Start 1002..1005 in parallel
    start_top_children = []
    for pane, idx in zip(top_row[1:], node_indices_top[1:]):
        start_top_children.append(send_cmd(pane, child_daemon_cmd(idx)))
    
    await asyncio.gather(*start_top_children)
    
    # after 10s, list_mem on 1001..1005
    await asyncio.sleep(10)
    list_top = []
    for pane in top_row:
        list_top.append(send_cmd(pane, "\n\n"))
        list_top.append(send_cmd(pane, list_mem_cmd()))
    
    await asyncio.gather(*list_top)
    
    # —— wait 15 seconds before starting the bottom row ——
    await asyncio.sleep(15)
    
    # 2c) Start 1006..1010 in parallel
    start_bottom = []
    for pane, idx in zip(bottom_row, node_indices_bottom):
        start_bottom.append(send_cmd(pane, child_daemon_cmd(idx)))
    
    await asyncio.gather(*start_bottom)
    
    # give them time to join/settle
    await asyncio.sleep(7)
    
    # Final list_mem sweep on all panes (parallel)
    final_sweep = []
    for pane in top_row + bottom_row:
        final_sweep.append(send_cmd(pane, "\n\n"))
        final_sweep.append(send_cmd(pane, list_mem_cmd()))
        final_sweep.append(send_cmd(pane, "\n"))
    
    await asyncio.gather(*final_sweep)
    
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


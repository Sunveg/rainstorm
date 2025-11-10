#!/usr/bin/env python3
import asyncio
import iterm2
import os
import subprocess

WORKDIR = os.path.expanduser("~/Documents/Distributed_MP/MP3_sunveg_clone/MP3")
BASE_CMD = "./hydfs"
BASE_HOST = "127.0.0.1"
BASE_PORT = 8001
REF_ADDR = f"{BASE_HOST}:{BASE_PORT}"

def base_daemon():
    return f'cd "{WORKDIR}" && {BASE_CMD} {BASE_HOST} {BASE_PORT}'

def child_daemon_port(port: int):
    return f'cd "{WORKDIR}" && {BASE_CMD} {BASE_HOST} {port} {REF_ADDR}'

def list_mem_cmd():
    return "membership"

# async def send_enters(session, n=1):
#     # Simulate hitting Enter n times (blank lines / spacing)
#     await session.async_send_text("\n" * 4)

    

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

    # --- TOP ROW: start 8001..8005 ---
    await top_row[0].async_send_text(base_daemon() + "\n")  # 8001

    # wait a bit, then list_mem on 8001
    await asyncio.sleep(2)
    await top_row[0].async_send_text("\n\n")  # spacing
    await top_row[0].async_send_text(list_mem_cmd() + "\n")

    # start 8002..8005
    for s, p in zip(top_row[1:], [8002, 8003, 8004, 8005]):
        await s.async_send_text(child_daemon_port(p) + "\n")


    
    # after 10s, run list_mem on 8001..8005
    await asyncio.sleep(10)
    for s in top_row[:]:
        # spaces
        await s.async_send_text("\n\n")
        await s.async_send_text(list_mem_cmd() + "\n")

    # —— wait 20 seconds before starting the bottom row ——
    await asyncio.sleep(15)

    # --- BOTTOM ROW: start 8006..8010 ---
    for s, p in zip(bottom_row, [8006, 8007, 8008, 8009, 8010]):
        await s.async_send_text(child_daemon_port(p) + "\n")

    await asyncio.sleep(7)


    for s in top_row + bottom_row:
        await s.async_send_text("\n\n")        # spacing (optional)
        await s.async_send_text(list_mem_cmd() + "\n")
        await s.async_send_text("\n")          # spacing (optional)

    # Make panes equal (View → Arrange Panes → Split Panes Evenly)
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

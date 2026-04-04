#!/usr/bin/env python3
"""
PharmaCentral Quick Start
=========================
This script starts the PharmaCentral server.
No external dependencies required — runs on Python 3.10+ stdlib only.
"""
import subprocess
import sys
import os
import webbrowser
import time

script_dir = os.path.dirname(os.path.abspath(__file__))
server_path = os.path.join(script_dir, "pharmacentral", "server.py")

print("""
╔══════════════════════════════════════════════════════╗
║         PharmaCentral — Pharmacy Chain Ops           ║
║         Full Working Prototype v1.0                  ║
╚══════════════════════════════════════════════════════╝

Starting server...
""")

proc = subprocess.Popen([sys.executable, server_path], cwd=script_dir)
time.sleep(2)

print("""
✅ Server running at: http://localhost:8000

👥 Login Credentials:
   ┌─────────────┬──────────────┬──────────────────────────┐
   │ Username    │ Password     │ Role                     │
   ├─────────────┼──────────────┼──────────────────────────┤
   │ admin       │ password123  │ Head Office Admin        │
   │ rajesh      │ password123  │ Pharmacist (MG Road)     │
   │ vikram      │ password123  │ Regional Manager         │
   │ sunita      │ password123  │ Inventory Controller     │
   └─────────────┴──────────────┴──────────────────────────┘

🚀 Opening browser...
Press Ctrl+C to stop the server.
""")

time.sleep(1)
try:
    webbrowser.open("http://localhost:8000")
except:
    pass

try:
    proc.wait()
except KeyboardInterrupt:
    proc.terminate()
    print("\n⛔ PharmaCentral stopped.")

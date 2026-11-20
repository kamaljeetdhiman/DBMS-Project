#!/usr/bin/env python3
"""
TimescaleDB vs. PostgreSQL 16 IoT Benchmark Dashboard Launcher
NIT Jalandhar - Department of Computer Science and Engineering
Group G13: Khushpreet Kaur, Krish Sharma, Kamaljeet Singh, Jitendra Chaudhary
"""

import http.server
import socketserver
import webbrowser
import os
import sys
import threading
import time

PORT = 8080

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    """Custom request handler with clean logging and proper MIME types."""
    def log_message(self, format, *args):
        # Keep terminal output clean and readable
        sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {args[0]} - {args[1]}\n")

def find_available_port(start_port=8080, max_attempts=10):
    """Finds an open port starting from start_port."""
    import socket
    for port in range(start_port, start_port + max_attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('', port))
                return port
            except OSError:
                continue
    return start_port

def open_browser(url):
    """Opens the user's default browser after a brief delay."""
    time.sleep(0.6)
    webbrowser.open(url)

def main():
    # Ensure working directory is the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    port = find_available_port(PORT)
    url = f"http://localhost:{port}"

    print("=" * 66)
    print("  DR. B. R. AMBEDKAR NATIONAL INSTITUTE OF TECHNOLOGY JALANDHAR")
    print("  Department of Computer Science and Engineering")
    print("  Course: Database Management Systems (CSX-208)")
    print("-" * 66)
    print("  TIMESCALEDB VS. POSTGRESQL 16 BENCHMARK DASHBOARD")
    print("  Group G13: Khushpreet Kaur, Krish Sharma,")
    print("             Kamaljeet Singh, Jitendra Chaudhary")
    print("=" * 66)
    print(f"\n[+] Local evaluation server running at: {url}")
    print("[+] Launching default web browser...")
    print("[+] Press Ctrl+C in this terminal window to stop the server.\n")

    # Start browser in a background thread
    threading.Thread(target=open_browser, args=(url,), daemon=True).start()

    # Serve files
    try:
        with socketserver.TCPServer(("", port), QuietHandler) as httpd:
            httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[+] Dashboard server stopped cleanly.")
    except Exception as e:
        print(f"\n[!] Server error: {e}")
        print("    You can still open index.html directly in any web browser.")

if __name__ == "__main__":
    main()

#!/usr/bin/env bash
# Quick launcher for TimescaleDB vs PostgreSQL 16 Dashboard (Group G13)
cd "$(dirname "$0")" || exit 1

if command -v python3 &>/dev/null; then
    python3 app.py
elif command -v python &>/dev/null; then
    python app.py
else
    echo "Python not detected. Opening index.html in browser..."
    if command -v xdg-open &>/dev/null; then
        xdg-open index.html
    elif command -v open &>/dev/null; then
        open index.html
    else
        echo "Please open index.html directly in any modern web browser."
    fi
fi

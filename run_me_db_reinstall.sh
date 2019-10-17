#!/usr/bin/env bash
# ensure running as root                                                        
if [ "$(id -u)" != "0" ]; then
  exec sudo "$0" "$@"
fi

sudo python3 refresh_db/db_reinstall.py
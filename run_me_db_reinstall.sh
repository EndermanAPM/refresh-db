#!/usr/bin/env bash
# ensure running as root                                                        
if [ "$(id -u)" != "0" ]; then
  exec sudo "$0" "$@"
fi

cd refresh_db
sudo python3 db_reinstall.py
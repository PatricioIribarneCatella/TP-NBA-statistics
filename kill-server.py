#!/usr/bin/python3

import signal
import os

with open("pids.store") as f:
    
    lines = f.readlines()
    
    for pid in lines:
        try:
            os.kill(int(pid), signal.SIGKILL)
        except ProcessLookupError:
            pass


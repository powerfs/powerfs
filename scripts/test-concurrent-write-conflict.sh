#!/bin/bash
# Concurrency write conflict test for FUSE cap/lock mechanism
# Strategy:
#   1. fuse-1 opens /mnt/powerfs/holdtest with O_WRONLY, writes data, holds fd for 8s
#      (this keeps an EXCLUSIVE cap granted on inode)
#   2. fuse-2 attempts to open /mnt/powerfs/holdtest with O_WRONLY and write during
#      the hold window, which should trigger server-side GATHER recall against fuse-1
#   3. After both sides finish, we check fuse
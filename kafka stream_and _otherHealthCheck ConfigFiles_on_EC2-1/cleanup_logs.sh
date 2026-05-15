#!/bin/bash

find /home/ubuntu -name "*.log" -type f -size +100M -exec truncate -s 0 {} \;

echo "Log cleanup completed at $(date)"

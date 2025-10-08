#!/usr/bin/env bash

# get cpu and mem usage of containers
set -euo pipefail
if [ "$#" -lt 1 ]; then
	echo "usage: $0 <container> [<container>...]" >&2
	exit 1
fi

# print csv header
echo "ts,container,cpu_perc,mem_usage"
while true; do
	ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" "$@" |
		while IFS= read -r line; do
			echo "$ts,$line"
		done
	sleep 1
done

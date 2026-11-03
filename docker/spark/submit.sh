#!/usr/bin/env bash
# spark-submit wrapper: advertise this container's IP as the driver host so the
# executors on the worker containers can call back into the driver.
exec spark-submit --conf "spark.driver.host=$(hostname -i | awk '{print $1}')" "$@"

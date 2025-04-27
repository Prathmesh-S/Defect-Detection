#!/usr/bin/env bash

# Set bash strict mode
set -euo pipefail

# Configuration variables
FLINK_HOME=${FLINK_HOME:-/opt/flink}
CONFIG_DIR=${CONFIG_DIR:-"${FLINK_HOME}/conf"}
LOG_DIR=${LOG_DIR:-"${FLINK_HOME}/log"}

# Display execution info
echo "Starting Flink entrypoint script"
echo "Command: $@"

# Process the first argument (mode selection)
MODE=$1
shift

# Setup default JVM options
if [ -z "${FLINK_ENV_JAVA_OPTS}" ]; then
    FLINK_ENV_JAVA_OPTS=""
fi

# Prepare environment for Flink
prepare_flink_env() {
    # Apply any custom configuration files
    if [ -d "${CONFIG_DIR}" ]; then
        echo "Using configuration directory: ${CONFIG_DIR}"
    fi

    # Ensure log directory exists
    mkdir -p "${LOG_DIR}"
}

# Process Flink properties from environment variable
apply_flink_properties() {
    if [ -n "${FLINK_PROPERTIES}" ]; then
        echo "Applying custom Flink properties"
        echo "${FLINK_PROPERTIES}" >> "${CONFIG_DIR}/flink-conf.yaml"
    fi
}

# Main execution based on mode
case $MODE in
    # Start a standalone Flink job
    standalone-job)
        prepare_flink_env
        apply_flink_properties

        echo "Starting Flink standalone job"

        # Parse job arguments
        JOB_CLASSNAME=""
        JOB_ARGS=""

        while [[ $# -gt 0 ]]; do
            case $1 in
                --job-classname)
                    JOB_CLASSNAME="$2"
                    shift 2
                    ;;
                --job-args)
                    JOB_ARGS="$2"
                    shift 2
                    ;;
                *)
                    echo "Unknown option: $1"
                    exit 1
                    ;;
            esac
        done

        if [ -z "${JOB_CLASSNAME}" ]; then
            echo "Error: --job-classname must be specified"
            exit 1
        fi

        echo "Starting job ${JOB_CLASSNAME} with args: ${JOB_ARGS}"
        exec "${FLINK_HOME}/bin/standalone-job.sh" start-foreground \
            -Djobmanager.memory.process.size=1600m \
            -Dtaskmanager.memory.process.size=1728m \
            -Drest.flamegraph.enabled=true \
            --job-classname "${JOB_CLASSNAME}" ${JOB_ARGS}
        ;;

    # Start JobManager
    jobmanager)
        prepare_flink_env
        apply_flink_properties

        echo "Starting Flink JobManager"
        exec "${FLINK_HOME}/bin/jobmanager.sh" start-foreground "$@"
        ;;

    # Start TaskManager
    taskmanager)
        prepare_flink_env
        apply_flink_properties

        echo "Starting Flink TaskManager"
        exec "${FLINK_HOME}/bin/taskmanager.sh" start-foreground "$@"
        ;;

    # Help option
    help)
        echo "Usage: $(basename "$0") (standalone-job|jobmanager|taskmanager) [args]"
        exit 0
        ;;

    # Any other command is passed to /bin/bash
    *)
        args=("$MODE" "$@")
        exec /bin/bash "${args[@]}"
        ;;
esac
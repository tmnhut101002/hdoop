#!/bin/bash
/opt/bitnami/spark/scripts/config/install_dependencies.sh
exec "$@"
# Keep the container running
tail -f /dev/null
#!/bin/bash
set -euo pipefail

# detect VIRTUAL_ENV, then deactivate venv first
# a little counterintuitive: first load the activate script with
# the deactivate command removing all necessary settings
test -d "$VIRTUAL_ENV" && \
source $VIRTUAL_ENV/bin/activate && \
deactivate

# activate and start python
source "{{ conda_root_prefix }}/bin/activate" "{{ conda_env }}" && \
exec "$CONDA_PREFIX/bin/{{ interpreter_name }}" "$@"

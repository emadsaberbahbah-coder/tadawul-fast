Tadawul Fast Bridge — Production Process Configuration (v1.1.0)

============================================================

DESIGN NOTE:

This Procfile delegates execution to the hardened 'scripts/start_web.sh'.

BENEFITS:

1. Signal Safety: Ensures SIGTERM/SIGINT are handled gracefully by the script.

2. Adaptive Scaling: Allows the script to calculate workers based on available RAM/CPU.

3. Validation: Performs environment checks before the process actually binds to the port.

web: sh scripts/start_web.sh

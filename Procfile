Tadawul Fast Bridge — Production Process Configuration (v1.2.0)

============================================================

DESIGN NOTE:

This Procfile delegates execution to the hardened 'scripts/start_web.sh'.

BENEFITS:

1. Signal Safety: Ensures SIGTERM/SIGINT are handled gracefully by the script (using 'exec').

2. Adaptive Scaling: Allows the script to calculate optimal workers based on CPU cores.

3. Validation: Performs critical environment checks before binding to the port.

USAGE:

Render/Heroku will automatically use the 'web' process type defined below.

web: ./scripts/start_web.sh

#!/usr/bin/env bash
# ======================================================================
# TADAWUL FAST BRIDGE – ENTERPRISE WEB LAUNCHER (v4.5.0)
# ======================================================================
# Production-Grade Application Server with Advanced Orchestration
#
# Core Capabilities:
# • Intelligent process management with automatic worker scaling
# • Multi-mode execution (development/production/performance/benchmark)
# • Advanced health monitoring with self-healing
# • Dynamic configuration with hot-reload support
# • Performance profiling and metrics collection
# • Graceful shutdown with connection draining
# • Memory/CPU limits enforcement
# • Structured logging with multiple outputs
# • Chaos engineering experiments support
# • Distributed tracing integration
# • Automatic certificate management (Let's Encrypt)
# • Blue-green deployment support
# • Canary release capabilities
#
# Architecture:
#   ┌─────────────────────────────────────┐
#   │         Load Balancer                │
#   └───────────────┬─────────────────────┘
#                   │
#   ┌───────────────┴─────────────────────┐
#   │      Application Server (This)       │
#   ├─────────────────────────────────────┤
#   │ • Gunicorn (Multi-worker)           │
#   │ • Uvicorn (ASGI)                    │
#   │ • Nginx (Static/Reverse Proxy)      │
#   │ • Redis (Cache/Session)              │
#   │ • PostgreSQL (Primary DB)            │
#   └─────────────────────────────────────┘
#
# Exit Codes:
#   0: Clean exit
#   1: Configuration error
#   2: Runtime error
#   3: Worker timeout/failure
#   4: Health check failed
#   5: Resource exhaustion
#   6: Database connection failed
#   7: Cache connection failed
#   8: SSL/TLS configuration error
#   130: Interrupted by user
#
# Usage Examples:
#   # Development (hot reload)
#   ./scripts/start_web.sh --dev
#
#   # Production with custom workers
#   ./scripts/start_web.sh --workers 8 --worker-class httptools
#
#   # Performance mode with profiling
#   ./scripts/start_web.sh --perf --profile --profile-dir ./profiles
#
#   # With SSL/TLS
#   ./scripts/start_web.sh --ssl --ssl-cert ./certs/cert.pem --ssl-key ./certs/key.pem
#
#   # Blue-green deployment
#   ./scripts/start_web.sh --blue-green --version v2.0.0
#
#   # Chaos testing
#   ./scripts/start_web.sh --chaos --failure-rate 0.1 --latency 100ms
#
#   # Benchmark mode
#   ./scripts/start_web.sh --benchmark --requests 10000 --concurrency 100
#
#   # With external services
#   ./scripts/start_web.sh --redis redis://localhost:6379 --postgres postgresql://localhost:5432/db
#
# ======================================================================

set -euo pipefail
IFS=$'\n\t'

# ======================================================================
# Version Information
# ======================================================================
readonly SCRIPT_VERSION="4.5.0"
readonly SCRIPT_NAME="TFB Enterprise Web Launcher"
readonly MIN_BASH_VERSION="4.0"

# Check bash version
if (( BASH_VERSINFO[0] < 4 )); then
    echo "❌ Error: Bash 4.0 or higher required" >&2
    exit 1
fi

# ======================================================================
# Configuration (with defaults)
# ======================================================================

# Application settings
readonly APP_MODULE="${APP_MODULE:-main:app}"
readonly APP_DIR="${APP_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
readonly APP_ENV="${APP_ENV:-production}"
readonly APP_VERSION="${APP_VERSION:-$(git describe --tags --always 2>/dev/null || echo "dev")}"

# Server settings
readonly HOST="${HOST:-0.0.0.0}"
readonly PORT="${PORT:-8000}"
readonly WORKERS="${WORKERS:-}"
readonly WORKER_CLASS="${WORKER_CLASS:-}"
readonly WORKER_CONNECTIONS="${WORKER_CONNECTIONS:-1000}"
readonly WORKER_MAX_REQUESTS="${WORKER_MAX_REQUESTS:-0}"
readonly WORKER_MAX_REQUESTS_JITTER="${WORKER_MAX_REQUESTS_JITTER:-0}"
readonly WORKER_TIMEOUT="${WORKER_TIMEOUT:-120}"
readonly WORKER_GRACEFUL_TIMEOUT="${WORKER_GRACEFUL_TIMEOUT:-30}"
readonly WORKER_KEEPALIVE="${WORKER_KEEPALIVE:-5}"

# Performance tuning
readonly BACKLOG="${BACKLOG:-2048}"
readonly LIMIT_CONCURRENCY="${LIMIT_CONCURRENCY:-}"
readonly LIMIT_MAX_REQUESTS="${LIMIT_MAX_REQUESTS:-}"
readonly WEB_CONCURRENCY_MAX="${WEB_CONCURRENCY_MAX:-8}"
readonly WORKERS_PER_CORE="${WORKERS_PER_CORE:-2}"
readonly WORKERS_PER_GB="${WORKERS_PER_GB:-2}"

# Resource limits
readonly MAX_MEMORY_MB="${MAX_MEMORY_MB:-}"
readonly MAX_CPU_PERCENT="${MAX_CPU_PERCENT:-}"
readonly MAX_FD_LIMIT="${MAX_FD_LIMIT:-65536}"
readonly MEMORY_SOFT_LIMIT="${MEMORY_SOFT_LIMIT:-0.8}"
readonly MEMORY_HARD_LIMIT="${MEMORY_HARD_LIMIT:-0.95}"

# Timeouts
readonly TIMEOUT_KEEP_ALIVE="${TIMEOUT_KEEP_ALIVE:-65}"
readonly TIMEOUT_GRACEFUL="${TIMEOUT_GRACEFUL:-30}"
readonly TIMEOUT_REQUEST="${TIMEOUT_REQUEST:-60}"
readonly TIMEOUT_STARTUP="${TIMEOUT_STARTUP:-30}"
readonly TIMEOUT_SHUTDOWN="${TIMEOUT_SHUTDOWN:-30}"

# Logging
readonly LOG_LEVEL="${LOG_LEVEL:-info}"
readonly LOG_FORMAT="${LOG_FORMAT:-json}"
readonly LOG_FILE="${LOG_FILE:-}"
readonly LOG_MAX_BYTES="${LOG_MAX_BYTES:-104857600}"  # 100MB
readonly LOG_BACKUP_COUNT="${LOG_BACKUP_COUNT:-10}"
readonly ACCESS_LOG="${ACCESS_LOG:-true}"
readonly ACCESS_LOG_FORMAT="${ACCESS_LOG_FORMAT:-combined}"

# SSL/TLS
readonly SSL_CERT_FILE="${SSL_CERT_FILE:-}"
readonly SSL_KEY_FILE="${SSL_KEY_FILE:-}"
readonly SSL_CA_BUNDLE="${SSL_CA_BUNDLE:-}"
readonly SSL_MIN_VERSION="${SSL_MIN_VERSION:-TLSv1.2}"
readonly SSL_CIPHERS="${SSL_CIPHERS:-ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256}"

# External services
readonly REDIS_URL="${REDIS_URL:-}"
readonly POSTGRES_URL="${POSTGRES_URL:-}"
readonly MEMCACHED_URL="${MEMCACHED_URL:-}"
readonly RABBITMQ_URL="${RABBITMQ_URL:-}"
readonly ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-}"

# Monitoring
readonly METRICS_PORT="${METRICS_PORT:-9090}"
readonly HEALTH_CHECK_PATH="${HEALTH_CHECK_PATH:-/health}"
readonly HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-30}"
readonly HEALTH_CHECK_TIMEOUT="${HEALTH_CHECK_TIMEOUT:-5}"
readonly HEALTH_CHECK_UNHEALTHY_THRESHOLD="${HEALTH_CHECK_UNHEALTHY_THRESHOLD:-3}"

# Feature flags
readonly ENABLE_METRICS="${ENABLE_METRICS:-true}"
readonly ENABLE_TRACING="${ENABLE_TRACING:-false}"
readonly ENABLE_PROFILING="${ENABLE_PROFILING:-false}"
readonly ENABLE_CACHE="${ENABLE_CACHE:-true}"
readonly ENABLE_RATE_LIMITING="${ENABLE_RATE_LIMITING:-true}"
readonly ENABLE_CIRCUIT_BREAKER="${ENABLE_CIRCUIT_BREAKER:-true}"
readonly ENABLE_BLUE_GREEN="${ENABLE_BLUE_GREEN:-false}"
readonly ENABLE_CANARY="${ENABLE_CANARY:-false}"
readonly ENABLE_CHAOS="${ENABLE_CHAOS:-false}"

# Chaos engineering
readonly CHAOS_FAILURE_RATE="${CHAOS_FAILURE_RATE:-0.0}"
readonly CHAOS_LATENCY_MS="${CHAOS_LATENCY_MS:-0}"
readonly CHAOS_EXCEPTION_RATE="${CHAOS_EXCEPTION_RATE:-0.0}"

# Blue-green deployment
readonly BLUE_GREEN_CURRENT="${BLUE_GREEN_CURRENT:-blue}"
readonly BLUE_GREEN_UPSTREAM="${BLUE_GREEN_UPSTREAM:-unix:/tmp/blue.sock}"

# ======================================================================
# Utility Functions
# ======================================================================

# Colors for output
readonly COLOR_RESET='\033[0m'
readonly COLOR_RED='\033[0;31m'
readonly COLOR_GREEN='\033[0;32m'
readonly COLOR_YELLOW='\033[1;33m'
readonly COLOR_BLUE='\033[0;34m'
readonly COLOR_MAGENTA='\033[0;35m'
readonly COLOR_CYAN='\033[0;36m'
readonly COLOR_BOLD='\033[1m'

log_info() {
    echo -e "${COLOR_GREEN}ℹ${COLOR_RESET} $1" >&2
}

log_warn() {
    echo -e "${COLOR_YELLOW}⚠${COLOR_RESET} $1" >&2
}

log_error() {
    echo -e "${COLOR_RED}✗${COLOR_RESET} $1" >&2
}

log_success() {
    echo -e "${COLOR_GREEN}✓${COLOR_RESET} $1" >&2
}

log_debug() {
    if [[ "${LOG_LEVEL}" == "debug" ]]; then
        echo -e "${COLOR_BLUE}🔍${COLOR_RESET} $1" >&2
    fi
}

log_metric() {
    local name="$1"
    local value="$2"
    local tags="${3:-}"
    echo "METRIC|${name}|${value}|${tags}" >&2
}

# Safe integer parsing
safe_int() {
    local value="$1"
    local default="${2:-0}"
    if [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "$value"
    else
        echo "$default"
    fi
}

# Safe float parsing
safe_float() {
    local value="$1"
    local default="${2:-0}"
    if [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
        echo "$value"
    else
        echo "$default"
    fi
}

# Boolean parsing
parse_bool() {
    local value="$1"
    local default="${2:-false}"
    case "${value,,}" in
        1|true|yes|y|on) echo "true" ;;
        0|false|no|n|off) echo "false" ;;
        *) echo "$default" ;;
    esac
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Get number of CPU cores
get_cpu_cores() {
    if command_exists nproc; then
        nproc 2>/dev/null || echo "1"
    elif [[ -f /proc/cpuinfo ]]; then
        grep -c ^processor /proc/cpuinfo 2>/dev/null || echo "1"
    elif command_exists sysctl; then
        sysctl -n hw.ncpu 2>/dev/null || echo "1"
    else
        echo "1"
    fi
}

# Get available memory in MB
get_memory_mb() {
    if [[ -f /proc/meminfo ]]; then
        local mem_kb=$(grep MemAvailable /proc/meminfo 2>/dev/null | awk '{print $2}')
        if [[ -n "$mem_kb" ]]; then
            echo $((mem_kb / 1024))
            return
        fi
    fi
    
    if command_exists free; then
        free -m | awk '/^Mem:/ {print $7}' 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Get load average
get_load_average() {
    if [[ -f /proc/loadavg ]]; then
        cut -d' ' -f1 /proc/loadavg 2>/dev/null || echo "0"
    else
        uptime | awk -F'load average:' '{print $2}' | cut -d, -f1 | tr -d ' ' 2>/dev/null || echo "0"
    fi
}

# Calculate optimal workers
calculate_workers() {
    local cpu_cores=$(get_cpu_cores)
    local memory_mb=$(get_memory_mb)
    local max_workers=$(safe_int "${WEB_CONCURRENCY_MAX}" 8)
    
    # Calculate based on CPU
    local cpu_workers=$((cpu_cores * WORKERS_PER_CORE))
    
    # Calculate based on memory (assuming 200MB per worker)
    local memory_workers=0
    if [[ "$memory_mb" -gt 0 ]]; then
        memory_workers=$((memory_mb / 200))
    fi
    
    # Take minimum of CPU and memory based workers, capped at max
    local workers=$((cpu_workers < memory_workers ? cpu_workers : memory_workers))
    if [[ "$workers" -gt "$max_workers" ]]; then
        workers="$max_workers"
    fi
    
    # Ensure at least 1 worker
    if [[ "$workers" -lt 1 ]]; then
        workers=1
    fi
    
    echo "$workers"
}

# Calculate optimal worker class
get_worker_class() {
    if [[ -n "${WORKER_CLASS}" ]]; then
        echo "${WORKER_CLASS}"
    elif python -c "import uvloop" 2>/dev/null; then
        echo "uvicorn.workers.UvicornWorker"
    else
        echo "uvicorn.workers.UvicornH11Worker"
    fi
}

# ======================================================================
# Resource Monitoring
# ======================================================================

check_resource_limits() {
    log_debug "Checking resource limits..."
    
    # Check file descriptor limits
    local fd_limit=$(ulimit -n)
    if [[ "$fd_limit" -lt "$MAX_FD_LIMIT" ]]; then
        log_warn "File descriptor limit ($fd_limit) is low. Consider increasing to $MAX_FD_LIMIT"
        ulimit -n "$MAX_FD_LIMIT" 2>/dev/null || true
    fi
    
    # Check memory limits
    if [[ -n "$MAX_MEMORY_MB" ]]; then
        local memory_mb=$(get_memory_mb)
        if [[ "$memory_mb" -gt 0 ]] && [[ "$memory_mb" -lt "$MAX_MEMORY_MB" ]]; then
            log_error "Insufficient memory: ${memory_mb}MB available, ${MAX_MEMORY_MB}MB required"
            return 5
        fi
    fi
    
    # Check CPU limits
    if [[ -n "$MAX_CPU_PERCENT" ]]; then
        local load_avg=$(get_load_average)
        local cpu_cores=$(get_cpu_cores)
        local cpu_percent=$(echo "scale=2; $load_avg / $cpu_cores * 100" | bc 2>/dev/null || echo "0")
        
        if (( $(echo "$cpu_percent > $MAX_CPU_PERCENT" | bc -l) )); then
            log_warn "CPU usage (${cpu_percent}%) exceeds limit (${MAX_CPU_PERCENT}%)"
        fi
    fi
    
    log_success "Resource limits OK"
    return 0
}

# ======================================================================
# Health Checks
# ======================================================================

check_python_environment() {
    log_debug "Checking Python environment..."
    
    # Check Python version
    local python_version=$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null || echo "0.0")
    if [[ "$python_version" < "3.8" ]]; then
        log_error "Python 3.8+ required (found $python_version)"
        return 1
    fi
    
    # Check critical packages
    local missing_packages=()
    for pkg in uvicorn fastapi pydantic; do
        if ! python -c "import $pkg" 2>/dev/null; then
            missing_packages+=("$pkg")
        fi
    done
    
    if [[ ${#missing_packages[@]} -gt 0 ]]; then
        log_error "Missing required packages: ${missing_packages[*]}"
        return 1
    fi
    
    log_success "Python environment OK"
    return 0
}

check_app_import() {
    log_debug "Checking application import..."
    
    # Extract module and app name
    local module="${APP_MODULE%%:*}"
    local app="${APP_MODULE##*:}"
    
    if ! python -c "import $module; getattr($module, '$app')" 2>/dev/null; then
        log_error "Failed to import $APP_MODULE"
        return 1
    fi
    
    log_success "Application import OK"
    return 0
}

check_database() {
    if [[ -z "${POSTGRES_URL}" ]]; then
        log_debug "No database configured, skipping check"
        return 0
    fi
    
    log_debug "Checking database connection..."
    
    if command_exists psql; then
        if ! PGCONNECT_TIMEOUT=5 psql "${POSTGRES_URL}" -c "SELECT 1" >/dev/null 2>&1; then
            log_error "Database connection failed"
            return 6
        fi
    elif command_exists python; then
        if ! python -c "
import sys
try:
    from sqlalchemy import create_engine
    engine = create_engine('${POSTGRES_URL}')
    conn = engine.connect()
    conn.close()
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
            log_error "Database connection failed"
            return 6
        fi
    fi
    
    log_success "Database connection OK"
    return 0
}

check_redis() {
    if [[ -z "${REDIS_URL}" ]]; then
        log_debug "No Redis configured, skipping check"
        return 0
    fi
    
    log_debug "Checking Redis connection..."
    
    if command_exists redis-cli; then
        if ! timeout 5 redis-cli -u "${REDIS_URL}" ping >/dev/null 2>&1; then
            log_error "Redis connection failed"
            return 7
        fi
    fi
    
    log_success "Redis connection OK"
    return 0
}

check_disk_space() {
    log_debug "Checking disk space..."
    
    local disk_usage=$(df -P . | awk 'NR==2 {print $5}' | sed 's/%//')
    if [[ "$disk_usage" -gt 90 ]]; then
        log_warn "Disk usage is at ${disk_usage}%"
    elif [[ "$disk_usage" -gt 95 ]]; then
        log_error "Disk usage is critical at ${disk_usage}%"
        return 1
    fi
    
    log_success "Disk space OK"
    return 0
}

# ======================================================================
# SSL/TLS Configuration
# ======================================================================

setup_ssl() {
    if [[ -z "$SSL_CERT_FILE" ]] || [[ -z "$SSL_KEY_FILE" ]]; then
        return 0
    fi
    
    log_info "Setting up SSL/TLS..."
    
    # Check certificate files
    if [[ ! -f "$SSL_CERT_FILE" ]]; then
        log_error "SSL certificate not found: $SSL_CERT_FILE"
        return 8
    fi
    
    if [[ ! -f "$SSL_KEY_FILE" ]]; then
        log_error "SSL key not found: $SSL_KEY_FILE"
        return 8
    fi
    
    # Check certificate expiry
    if command_exists openssl; then
        local expiry=$(openssl x509 -enddate -noout -in "$SSL_CERT_FILE" | cut -d= -f2)
        local expiry_epoch=$(date -d "$expiry" +%s 2>/dev/null)
        local now_epoch=$(date +%s)
        
        if [[ "$expiry_epoch" -lt "$now_epoch" ]]; then
            log_error "SSL certificate has expired"
            return 8
        fi
        
        local days_left=$(( (expiry_epoch - now_epoch) / 86400 ))
        if [[ "$days_left" -lt 30 ]]; then
            log_warn "SSL certificate expires in $days_left days"
        fi
    fi
    
    # Set SSL environment variables for uvicorn/gunicorn
    export SSL_CERTFILE="$SSL_CERT_FILE"
    export SSL_KEYFILE="$SSL_KEY_FILE"
    export SSL_CA_BUNDLE="$SSL_CA_BUNDLE"
    
    log_success "SSL/TLS configured"
    return 0
}

# ======================================================================
# External Services
# ======================================================================

setup_external_services() {
    log_info "Setting up external services..."
    
    # Redis
    if [[ -n "$REDIS_URL" ]]; then
        export REDIS_URL
        log_debug "Redis configured: ${REDIS_URL%%@*}"
    fi
    
    # PostgreSQL
    if [[ -n "$POSTGRES_URL" ]]; then
        export DATABASE_URL="$POSTGRES_URL"
        log_debug "PostgreSQL configured: ${POSTGRES_URL%%@*}"
    fi
    
    # Memcached
    if [[ -n "$MEMCACHED_URL" ]]; then
        export MEMCACHED_URL
        log_debug "Memcached configured: $MEMCACHED_URL"
    fi
    
    # RabbitMQ
    if [[ -n "$RABBITMQ_URL" ]]; then
        export RABBITMQ_URL
        log_debug "RabbitMQ configured: ${RABBITMQ_URL%%@*}"
    fi
    
    # Elasticsearch
    if [[ -n "$ELASTICSEARCH_URL" ]]; then
        export ELASTICSEARCH_URL
        log_debug "Elasticsearch configured: $ELASTICSEARCH_URL"
    fi
    
    log_success "External services configured"
}

# ======================================================================
# Monitoring Setup
# ======================================================================

setup_monitoring() {
    if [[ "$ENABLE_METRICS" != "true" ]]; then
        return 0
    fi
    
    log_info "Setting up monitoring..."
    
    # Start metrics server in background
    if command_exists socat; then
        (
            while true; do
                echo -e "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n$(generate_metrics)" | \
                    socat -v -t 60 TCP-LISTEN:${METRICS_PORT},reuseaddr,fork STDIO >/dev/null 2>&1
            done
        ) &
        local metrics_pid=$!
        log_debug "Metrics server started on port ${METRICS_PORT} (PID: ${metrics_pid})"
    else
        log_warn "socat not available, metrics server disabled"
    fi
    
    log_success "Monitoring configured"
}

generate_metrics() {
    cat <<EOF
# HELP app_info Application information
# TYPE app_info gauge
app_info{version="${APP_VERSION}",environment="${APP_ENV}"} 1

# HELP app_uptime_seconds Application uptime in seconds
# TYPE app_uptime_seconds gauge
app_uptime_seconds $(($(date +%s) - $(start_time_epoch)))

# HELP app_workers_total Total number of workers
# TYPE app_workers_total gauge
app_workers_total ${WORKER_COUNT:-0}

# HELP app_memory_bytes Memory usage in bytes
# TYPE app_memory_bytes gauge
app_memory_bytes $(get_memory_usage)

# HELP app_cpu_usage_percent CPU usage percentage
# TYPE app_cpu_usage_percent gauge
app_cpu_usage_percent $(get_cpu_usage)

# HELP app_requests_total Total requests processed
# TYPE app_requests_total counter
app_requests_total $(get_request_count)

# HELP app_request_duration_seconds Request duration histogram
# TYPE app_request_duration_seconds histogram
app_request_duration_seconds_bucket{le="0.1"} $(get_request_count_bucket 0.1)
app_request_duration_seconds_bucket{le="0.5"} $(get_request_count_bucket 0.5)
app_request_duration_seconds_bucket{le="1"} $(get_request_count_bucket 1)
app_request_duration_seconds_bucket{le="5"} $(get_request_count_bucket 5)
app_request_duration_seconds_bucket{le="+Inf"} $(get_request_count_total)
app_request_duration_seconds_sum $(get_request_duration_sum)
app_request_duration_seconds_count $(get_request_count_total)
EOF
}

# ======================================================================
# Chaos Engineering
# ======================================================================

setup_chaos() {
    if [[ "$ENABLE_CHAOS" != "true" ]]; then
        return 0
    fi
    
    log_warn "CHAOS ENGINEERING ENABLED"
    log_warn "  Failure rate: ${CHAOS_FAILURE_RATE}"
    log_warn "  Latency: ${CHAOS_LATENCY_MS}ms"
    log_warn "  Exception rate: ${CHAOS_EXCEPTION_RATE}"
    
    # Set chaos environment variables
    export CHAOS_FAILURE_RATE
    export CHAOS_LATENCY_MS
    export CHAOS_EXCEPTION_RATE
    
    # Create chaos middleware if not exists
    local chaos_middleware="${APP_DIR}/chaos_middleware.py"
    if [[ ! -f "$chaos_middleware" ]] && [[ -w "$APP_DIR" ]]; then
        cat > "$chaos_middleware" <<'EOF'
"""Chaos engineering middleware"""
import asyncio
import random
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

class ChaosMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        failure_rate: float = 0.0,
        latency_ms: int = 0,
        exception_rate: float = 0.0
    ):
        super().__init__(app)
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.exception_rate = exception_rate
    
    async def dispatch(self, request, call_next):
        # Inject latency
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000)
        
        # Inject failures
        if random.random() < self.failure_rate:
            from starlette.responses import Response
            return Response("Chaos: Service Unavailable", status_code=503)
        
        # Inject exceptions
        if random.random() < self.exception_rate:
            raise Exception("Chaos: Injected exception")
        
        return await call_next(request)
EOF
        log_debug "Created chaos middleware"
    fi
}

# ======================================================================
# Blue-Green Deployment
# ======================================================================

setup_blue_green() {
    if [[ "$ENABLE_BLUE_GREEN" != "true" ]]; then
        return 0
    fi
    
    log_info "Setting up blue-green deployment..."
    
    # Determine current deployment
    if [[ "$BLUE_GREEN_CURRENT" == "blue" ]]; then
        export BLUE_SOCK="/tmp/blue.sock"
        export GREEN_SOCK="/tmp/green.sock"
        export CURRENT_SOCK="$BLUE_SOCK"
        export STANDBY_SOCK="$GREEN_SOCK"
    else
        export BLUE_SOCK="/tmp/green.sock"
        export GREEN_SOCK="/tmp/blue.sock"
        export CURRENT_SOCK="$GREEN_SOCK"
        export STANDBY_SOCK="$BLUE_SOCK"
    fi
    
    # Create socket directory
    mkdir -p /tmp/sockets
    
    # Set socket permissions
    chmod 755 /tmp/sockets
    
    # Configure uvicorn to use socket
    export UVICORN_FD="3"
    
    log_success "Blue-green configured (current: ${BLUE_GREEN_CURRENT})"
}

# ======================================================================
# Nginx Integration (for static files)
# ======================================================================

setup_nginx() {
    if ! command_exists nginx; then
        log_debug "Nginx not available, skipping"
        return 0
    fi
    
    log_info "Setting up Nginx reverse proxy..."
    
    # Generate nginx configuration
    local nginx_conf="/tmp/nginx-tfb.conf"
    cat > "$nginx_conf" <<EOF
daemon off;
worker_processes auto;
error_log /dev/stderr warn;
pid /tmp/nginx.pid;

events {
    worker_connections 1024;
    use epoll;
    multi_accept on;
}

http {
    include /etc/nginx/mime.types;
    default_type application/octet-stream;
    
    # Logging
    log_format main '\$remote_addr - \$remote_user [\$time_local] "\$request" '
                    '\$status \$body_bytes_sent "\$http_referer" '
                    '"\$http_user_agent" "\$http_x_forwarded_for"';
    access_log /dev/stdout main;
    
    # Performance
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    types_hash_max_size 2048;
    client_max_body_size 100M;
    server_tokens off;
    
    # Compression
    gzip on;
    gzip_vary on;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_types text/plain text/css text/xml application/json application/javascript application/xml+rss application/atom+xml image/svg+xml;
    
    # Upstream
    upstream app_server {
        server unix:/tmp/app.sock fail_timeout=0;
    }
    
    server {
        listen ${PORT};
        server_name _;
        
        # Health check
        location ${HEALTH_CHECK_PATH} {
            access_log off;
            proxy_pass http://app_server;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
        }
        
        # Static files
        location /static/ {
            alias ${APP_DIR}/static/;
            expires 30d;
            add_header Cache-Control "public, immutable";
        }
        
        # Media files
        location /media/ {
            alias ${APP_DIR}/media/;
        }
        
        # Main application
        location / {
            proxy_pass http://app_server;
            proxy_http_version 1.1;
            proxy_set_header Upgrade \$http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_set_header X-Forwarded-Host \$server_name;
            proxy_buffering off;
            proxy_cache off;
        }
    }
}
EOF
    
    # Start nginx
    nginx -c "$nginx_conf" &
    local nginx_pid=$!
    log_debug "Nginx started (PID: ${nginx_pid})"
    
    export NGINX_PID="$nginx_pid"
    log_success "Nginx configured"
}

# ======================================================================
# Gunicorn/Uvicorn Runner
# ======================================================================

run_gunicorn() {
    local worker_count="$1"
    local worker_class="$2"
    
    log_info "Starting Gunicorn with ${worker_count} workers (${worker_class})"
    
    # Build gunicorn command
    local cmd=(
        gunicorn
        "$APP_MODULE"
        --bind "unix:/tmp/app.sock"
        --workers "$worker_count"
        --worker-class "$worker_class"
        --worker-connections "$WORKER_CONNECTIONS"
        --max-requests "$WORKER_MAX_REQUESTS"
        --max-requests-jitter "$WORKER_MAX_REQUESTS_JITTER"
        --timeout "$WORKER_TIMEOUT"
        --graceful-timeout "$WORKER_GRACEFUL_TIMEOUT"
        --keepalive "$WORKER_KEEPALIVE"
        --backlog "$BACKLOG"
        --log-level "$LOG_LEVEL"
        --access-logfile -
        --error-logfile -
        --capture-output
        --enable-stdio-inheritance
        --reload
        --preload
    )
    
    # Add SSL if configured
    if [[ -n "$SSL_CERT_FILE" ]] && [[ -n "$SSL_KEY_FILE" ]]; then
        cmd+=(
            --certfile "$SSL_CERT_FILE"
            --keyfile "$SSL_KEY_FILE"
        )
        if [[ -n "$SSL_CA_BUNDLE" ]]; then
            cmd+=(--ca-certs "$SSL_CA_BUNDLE")
        fi
    fi
    
    # Add limit options
    if [[ -n "$LIMIT_CONCURRENCY" ]]; then
        cmd+=(--limit-concurrency "$LIMIT_CONCURRENCY")
    fi
    
    # Set Python path
    export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"
    
    # Execute
    exec "${cmd[@]}"
}

run_uvicorn() {
    local worker_count="$1"
    
    log_info "Starting Uvicorn with ${worker_count} workers"
    
    # Build uvicorn command
    local cmd=(
        python -m uvicorn
        "$APP_MODULE"
        --host "$HOST"
        --port "$PORT"
        --proxy-headers
        --forwarded-allow-ips "*"
        --lifespan on
        --timeout-keep-alive "$TIMEOUT_KEEP_ALIVE"
        --timeout-graceful-shutdown "$TIMEOUT_GRACEFUL"
        --log-level "$LOG_LEVEL"
        --no-server-header
        --date-header
    )
    
    # Add workers if >1
    if [[ "$worker_count" -gt 1 ]]; then
        cmd+=(--workers "$worker_count")
    fi
    
    # Add SSL if configured
    if [[ -n "$SSL_CERT_FILE" ]] && [[ -n "$SSL_KEY_FILE" ]]; then
        cmd+=(--ssl-certfile "$SSL_CERT_FILE" --ssl-keyfile "$SSL_KEY_FILE")
    fi
    
    # Add access log
    if [[ "$ACCESS_LOG" == "true" ]];

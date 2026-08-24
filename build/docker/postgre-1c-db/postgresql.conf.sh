#!/bin/bash
set -e

# Настройка postgresql.conf
cat >> "$PGDATA/postgresql.conf" <<EOF
listen_addresses = '*'
max_connections = 100
shared_buffers = 1GB
work_mem = 64MB
maintenance_work_mem = 512MB
log_min_duration_statement = 200ms
idle_in_transaction_session_timeout = 10s
lock_timeout = 1s
statement_timeout = 60s
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.max = 10000
pg_stat_statements.track = all
EOF

# Настройка pg_hba.conf
cat >> "$PGDATA/pg_hba.conf" <<EOF
host    all             all             0.0.0.0/0               md5
EOF
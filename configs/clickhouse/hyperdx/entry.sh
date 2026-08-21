#!/bin/bash
# Override stock entry.sh which forces IS_LOCAL_APP_MODE=REQUIRED_AUTH.
# Exact string required by HyperDX packages/api/build/config.js
export IS_LOCAL_APP_MODE="DANGEROUSLY_is_local_app_mode💀"
source "/etc/local/entry.base.sh"

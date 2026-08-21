"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const tslib_1 = require("tslib");
const sanitize_url_1 = require("@braintree/sanitize-url");
const express_1 = tslib_1.__importDefault(require("express"));
const http_proxy_middleware_1 = require("http-proxy-middleware");
const perf_hooks_1 = require("perf_hooks");
const zod_1 = require("zod");
const zod_express_middleware_1 = require("zod-express-middleware");
const config_1 = require("../../config");
const connection_1 = require("../../controllers/connection");
const auth_1 = require("../../middleware/auth");
const validation_1 = require("../../middleware/validation");
const instrumentation_1 = require("../../utils/instrumentation");
const logger_1 = tslib_1.__importDefault(require("../../utils/logger"));
const validators_1 = require("../../utils/validators");
const zod_2 = require("../../utils/zod");
// SLO operations for the ClickHouse proxy. Both paths swallow their errors
// (returning JSON / writing the response directly) so they never reach the API
// error middleware — they must report their own SLIs. See
// agent_docs/observability.md.
const CONNECTION_TEST_OPERATION = 'clickhouse_proxy.connection_test';
const QUERY_PROXY_OPERATION = 'clickhouse_proxy.query';
/**
 * Validates and sanitizes a URL path to prevent injection attacks.
 * - Recursively decodes to catch double/triple encoding of ? and &
 * - Rejects paths with encoded query string characters in pathname
 * - Prevents protocol-based attacks (javascript:, data:, etc.)
 * - Prevents host injection via protocol-relative URLs
 *
 * @param basePath - The path to validate (may include query string)
 * @returns Sanitized path with pathname and query string
 * @throws Error if path contains malicious patterns
 */
const validateAndSanitizePath = (basePath) => {
    // Extract pathname portion (before any literal ?) for encoding attack check
    // Must be done BEFORE sanitizeUrl because it decodes percent-encoded chars
    const firstQuestionMark = basePath.indexOf('?');
    const rawPathname = firstQuestionMark >= 0 ? basePath.slice(0, firstQuestionMark) : basePath;
    // Recursively decode pathname to prevent double-encoding attacks
    // (e.g., %253F -> %3F -> ?, %2526 -> %26 -> &)
    let decodedPathname = rawPathname;
    let prevDecoded = '';
    const maxIterations = 10; // Prevent infinite loops
    let iterations = 0;
    while (decodedPathname !== prevDecoded && iterations < maxIterations) {
        prevDecoded = decodedPathname;
        try {
            decodedPathname = decodeURIComponent(decodedPathname);
        }
        catch {
            throw new Error('Invalid pathname: malformed URL encoding');
        }
        iterations++;
    }
    // Validate fully-decoded pathname doesn't contain query string characters
    if (decodedPathname.includes('?') || decodedPathname.includes('&')) {
        throw new Error('Invalid pathname: contains query string characters');
    }
    // Sanitize URL to prevent protocol-based attacks (javascript:, data:, etc.)
    const sanitizedPath = (0, sanitize_url_1.sanitizeUrl)(basePath);
    if (sanitizedPath === 'about:blank') {
        throw new Error('Invalid pathname: potentially malicious URL');
    }
    // Use URL parsing to properly separate pathname from query params
    const parsedUrl = new URL(sanitizedPath, 'http://localhost');
    // Prevent host injection via protocol-relative URLs (e.g., //evil.com)
    if (parsedUrl.hostname !== 'localhost') {
        throw new Error('Invalid pathname: host injection attempt');
    }
    return `${parsedUrl.pathname}${parsedUrl.search}`;
};
const router = express_1.default.Router();
const CUSTOM_SETTING_KEY_SEP = '_';
const CUSTOM_SETTING_KEY_USER_SUFFIX = 'user';
router.post('/test', (0, zod_express_middleware_1.validateRequest)({
    body: zod_1.z.object({
        host: zod_1.z.string().url(),
        username: zod_1.z.string().optional(),
        password: zod_1.z.string().optional(),
    }),
}), async (req, res) => {
    const { host, username, password } = req.body;
    // Restrict to http/https to prevent file://, gopher://, etc.
    const parsedHost = new URL(host);
    if (parsedHost.protocol !== 'http:' && parsedHost.protocol !== 'https:') {
        return res
            .status(400)
            .json({ success: false, error: 'Invalid protocol' });
    }
    const hostname = parsedHost.hostname.replace(validators_1.IPV6_BRACKET_RE, '');
    if ((0, validators_1.isPrivateIp)(hostname)) {
        return res.status(400).json({ success: false, error: 'Invalid host' });
    }
    const startedAt = perf_hooks_1.performance.now();
    try {
        const result = await fetch(`${host}/?query=SELECT 1`, {
            headers: {
                'X-ClickHouse-User': username || '',
                'X-ClickHouse-Key': password || '',
            },
            signal: AbortSignal.timeout(2000),
        });
        // For status codes 204-399
        if (!result.ok) {
            (0, instrumentation_1.recordOperationOutcome)({
                operation: CONNECTION_TEST_OPERATION,
                outcome: 'error',
                durationMs: perf_hooks_1.performance.now() - startedAt,
            });
            // Do not reflect the raw response body to avoid leaking internal
            // service responses in case of a misconfigured or SSRF host.
            return res.status(result.status).json({
                success: false,
                error: 'Error connecting to ClickHouse server',
            });
        }
        const data = await result.json();
        (0, instrumentation_1.recordOperationOutcome)({
            operation: CONNECTION_TEST_OPERATION,
            outcome: 'success',
            durationMs: perf_hooks_1.performance.now() - startedAt,
        });
        return res.json({ success: data === 1 });
    }
    catch (e) {
        (0, instrumentation_1.recordOperationOutcome)({
            operation: CONNECTION_TEST_OPERATION,
            outcome: 'error',
            durationMs: perf_hooks_1.performance.now() - startedAt,
        });
        // fetch returns a 400+ error and throws
        console.error(e);
        const errorMessage = e.cause?.code === 'ENOTFOUND'
            ? `Unable to resolve host: ${e.cause.hostname}`
            : e.cause?.message ||
                e.message ||
                'Error connecting to ClickHouse server';
        return res.status(500).json({
            success: false,
            error: errorMessage +
                ', please check the host and credentials and try again.',
        });
    }
});
const hasConnectionId = (0, validation_1.validateRequestHeaders)(zod_1.z.object({
    'x-hyperdx-connection-id': zod_2.objectIdSchema,
}));
const getConnection = 
// prettier-ignore-next-line
async (req, res, next) => {
    try {
        const { teamId } = (0, auth_1.getNonNullUserWithTeam)(req);
        const connection_id = req.headers['x-hyperdx-connection-id']; // ! because zod already validated
        delete req.headers['x-hyperdx-connection-id'];
        const hyperdx_connection_id = Array.isArray(connection_id)
            ? connection_id.join('')
            : connection_id;
        const connection = await (0, connection_1.getConnectionById)(teamId.toString(), hyperdx_connection_id, true);
        if (!connection) {
            res.status(404).send('Connection not found');
            return;
        }
        req._hdx_connection = {
            host: connection.host,
            id: connection.id,
            name: connection.name,
            password: connection.password,
            username: connection.username,
            hyperdxSettingPrefix: connection.hyperdxSettingPrefix,
        };
        next();
    }
    catch (e) {
        console.error('Error setting up proxy hdx connection', e);
        next(e);
    }
};
const proxyMiddleware = 
// prettier-ignore-next-line
(0, http_proxy_middleware_1.createProxyMiddleware)({
    target: '', // doesn't matter. it should be overridden by the router
    changeOrigin: true,
    pathFilter: (path, _req) => {
        return _req.method === 'GET' || _req.method === 'POST';
    },
    pathRewrite: function (path, req) {
        const sanitizedPath = validateAndSanitizePath(path.replace(/^\/clickhouse-proxy/, ''));
        const parsedUrl = new URL(sanitizedPath, 'http://localhost');
        const { searchParams, pathname } = parsedUrl;
        // Append user email as custom ClickHouse setting for query log annotation if the prefix was set
        const hyperdxSettingPrefix = req._hdx_connection?.hyperdxSettingPrefix;
        if (hyperdxSettingPrefix) {
            const userEmail = req.user?.email;
            if (userEmail) {
                const userSettingKey = `${hyperdxSettingPrefix}${CUSTOM_SETTING_KEY_SEP}${CUSTOM_SETTING_KEY_USER_SUFFIX}`;
                searchParams.set(userSettingKey, userEmail);
            }
            else {
                logger_1.default.debug('hyperdxSettingPrefix set, no session user found');
            }
        }
        return `${pathname}?${searchParams.toString()}`;
    },
    router: _req => {
        if (!_req._hdx_connection?.host) {
            throw new Error('[createProxyMiddleware] Connection not found');
        }
        return _req._hdx_connection.host;
    },
    on: {
        proxyReq: (proxyReq, _req, res) => {
            // set user-agent to the hyperdx version identifier
            proxyReq.setHeader('user-agent', `hyperdx ${config_1.CODE_VERSION}`);
            // ClickHouse rejects mixing Authorization with X-ClickHouse-* headers.
            // @clickhouse/client may send Basic Auth; we authenticate only via X-ClickHouse-*.
            // See: https://github.com/hyperdxio/hyperdx/issues/962
            proxyReq.removeHeader('authorization');
            proxyReq.removeHeader('Authorization');
            if (_req._hdx_connection?.username) {
                proxyReq.setHeader('X-ClickHouse-User', _req._hdx_connection.username);
            }
            // Passwords can be empty
            if (_req._hdx_connection?.password) {
                proxyReq.setHeader('X-ClickHouse-Key', _req._hdx_connection.password);
            }
            if (_req.method !== 'POST') {
                console.error(`Unsupported method ${_req.method}`);
                return res.sendStatus(405);
            }
            let body = _req.body;
            if (_req.headers['content-type'] === 'application/json') {
                try {
                    body = JSON.stringify(body);
                }
                catch (e) {
                    console.error(e);
                }
            }
            try {
                proxyReq.write(body);
            }
            catch {
                console.error(`clickhouseProxy error writing body, body is type ${typeof body}`);
            }
        },
        proxyRes: (proxyRes, _req, res) => {
            const startedAt = res.locals?.hdxProxyStartedAt;
            const statusCode = proxyRes.statusCode ?? 0;
            (0, instrumentation_1.recordOperationOutcome)({
                operation: QUERY_PROXY_OPERATION,
                // A response (even a 4xx/5xx from ClickHouse) means the proxy hop
                // itself worked; outcome reflects whether ClickHouse served it.
                outcome: statusCode < 400 ? 'success' : 'error',
                durationMs: typeof startedAt === 'number' ? perf_hooks_1.performance.now() - startedAt : 0,
            });
            // since clickhouse v24, the cors headers * will be attached to the response by default
            // which will cause the browser to block the response
            if (_req.headers['access-control-request-method']) {
                proxyRes.headers['access-control-allow-methods'] =
                    _req.headers['access-control-request-method'];
            }
            if (_req.headers['access-control-request-headers']) {
                proxyRes.headers['access-control-allow-headers'] =
                    _req.headers['access-control-request-headers'];
            }
            if (_req.headers.origin) {
                proxyRes.headers['access-control-allow-origin'] = _req.headers.origin;
                proxyRes.headers['access-control-allow-credentials'] = 'true';
            }
        },
        error: (err, _req, _res) => {
            const startedAt = _res.locals?.hdxProxyStartedAt;
            (0, instrumentation_1.recordOperationOutcome)({
                operation: QUERY_PROXY_OPERATION,
                // No usable response from ClickHouse (connection refused, timeout,
                // DNS failure, ...) — a hard availability failure for the proxy.
                outcome: 'error',
                durationMs: typeof startedAt === 'number' ? perf_hooks_1.performance.now() - startedAt : 0,
            });
            console.error('Proxy error:', err);
            _res.writeHead(500, {
                'Content-Type': 'application/json',
            });
            _res.end(JSON.stringify({
                success: false,
                error: err.message || 'Failed to connect to ClickHouse server',
            }));
        },
    },
    // ...(config.IS_DEV && {
    //   logger: console,
    // }),
});
// Stamp a start time so the proxy callbacks can record query SLO latency.
const markProxyStart = (_req, res, next) => {
    res.locals.hdxProxyStartedAt = perf_hooks_1.performance.now();
    next();
};
router.get('/*', hasConnectionId, getConnection, markProxyStart, proxyMiddleware);
router.post('/*', hasConnectionId, getConnection, markProxyStart, proxyMiddleware);
exports.default = router;
//# sourceMappingURL=clickhouseProxy.js.map
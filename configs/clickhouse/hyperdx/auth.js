"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.redirectToDashboard = redirectToDashboard;
exports.handleAuthError = handleAuthError;
exports.getAccessKeyFromRequest = getAccessKeyFromRequest;
exports.validateUserAccessKey = validateUserAccessKey;
exports.isUserAuthenticated = isUserAuthenticated;
exports.getNonNullUserWithTeam = getNonNullUserWithTeam;
const tslib_1 = require("tslib");
const serialize_error_1 = require("serialize-error");
const config = tslib_1.__importStar(require("../config"));
const user_1 = require("../controllers/user");
const instrumentation_1 = require("../utils/instrumentation");
const logger_1 = tslib_1.__importDefault(require("../utils/logger"));
function redirectToDashboard(req, res) {
    // Use 303 See Other so browsers always follow the redirect with GET, even
    // when the original request was a POST (e.g. /login/password). Without an
    // explicit status, Express sends 302 and some browsers/proxies preserve the
    // POST method, which produces a 405 on Next.js pages that only accept GET.
    // The destination is the app root so client-side routing in LandingPage
    // decides where to send the user (/search if logged in, /login otherwise).
    // This avoids hard-coding /search here, which fails when the post-login
    // host differs from the configured FRONTEND_URL (e.g. Vercel previews).
    if (req?.user?.team) {
        return res.redirect(303, `${config.FRONTEND_REDIRECT_BASE}/`);
    }
    else {
        logger_1.default.error({ userId: req?.user?._id }, 'Password login for user failed, user or team not found');
        res.redirect(303, `${config.FRONTEND_REDIRECT_BASE}/login?err=unknown`);
    }
}
function handleAuthError(err, req, res, next) {
    logger_1.default.debug({ authErr: (0, serialize_error_1.serializeError)(err) }, 'Auth error');
    if (res.headersSent) {
        return next(err);
    }
    // Get the latest auth error message
    const lastMessage = req.session.messages?.at(-1);
    logger_1.default.debug(`Auth error last message: ${lastMessage}`);
    const returnErr = lastMessage === 'Password or username is incorrect'
        ? 'authFail'
        : lastMessage ===
            'Authentication method password is not allowed by your team admin.'
            ? 'passwordAuthNotAllowed'
            : 'unknown';
    // 303 forces GET on the redirected request even when the original request
    // was a POST (e.g. /login/password failure path).
    res.redirect(303, `${config.FRONTEND_REDIRECT_BASE}/login?err=${returnErr}`);
}
function getAccessKeyFromRequest(req) {
    return req.headers.authorization?.split('Bearer ')[1];
}
async function validateUserAccessKey(req, res, next) {
    // Local App Mode has no Mongo user / Personal API Access Key (hyperdx#1222).
    // MCP and External API v2 still go through this middleware, so reuse the
    // same synthetic identity as isUserAuthenticated.
    if (config.IS_LOCAL_APP_MODE) {
        logger_1.default.warn('Skipping access-key authentication in local app mode');
        req.user = {
            _id: '_local_user_',
            email: 'local-user@hyperdx.io',
            team: '_local_team_',
        };
        (0, instrumentation_1.setBusinessContext)({
            teamId: '_local_team_',
            userId: '_local_user_',
            'hyperdx.local_mode': true,
            ...(0, instrumentation_1.getStaticFeatureFlags)(),
        });
        return next();
    }
    const key = getAccessKeyFromRequest(req);
    if (!key) {
        return res.sendStatus(401);
    }
    const user = await (0, user_1.findUserByAccessKey)(key);
    if (!user) {
        return res.sendStatus(401);
    }
    req.user = user;
    // Attribute access-key authenticated requests (external API v2 + MCP HTTP)
    // with team/user context so their traces are searchable during incidents.
    (0, instrumentation_1.setBusinessContext)({
        teamId: user.team?.toString(),
        userId: user._id?.toString(),
        email: user.email,
        ...(0, instrumentation_1.getStaticFeatureFlags)(),
    });
    next();
}
function isUserAuthenticated(req, res, next) {
    if (config.IS_LOCAL_APP_MODE) {
        // If local app mode is enabled, skip authentication
        logger_1.default.warn('Skipping authentication in local app mode');
        req.user = {
            // @ts-expect-error local app mode uses a synthetic string id, not an ObjectId
            _id: '_local_user_',
            email: 'local-user@hyperdx.io',
            // @ts-expect-error local app mode uses a synthetic string team, not an ObjectId
            team: '_local_team_',
        };
        (0, instrumentation_1.setBusinessContext)({
            teamId: '_local_team_',
            userId: '_local_user_',
            'hyperdx.local_mode': true,
            ...(0, instrumentation_1.getStaticFeatureFlags)(),
        });
        return next();
    }
    if (req.isAuthenticated()) {
        // Attach incident-remediation context to the trace and active span.
        (0, instrumentation_1.setBusinessContext)({
            teamId: req.user?.team?.toString(),
            userId: req.user?._id?.toString(),
            email: req.user?.email,
            ...(0, instrumentation_1.getStaticFeatureFlags)(),
        });
        return next();
    }
    res.sendStatus(401);
}
function getNonNullUserWithTeam(req) {
    const user = req.user;
    if (!user) {
        throw new Error('User is not authenticated');
    }
    if (!user.team) {
        throw new Error(`User ${user._id} is not associated with a team`);
    }
    return { teamId: user.team, userId: user._id, email: user.email };
}
//# sourceMappingURL=auth.js.map
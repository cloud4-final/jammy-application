import crypto from 'k6/crypto';

export function sign(secret, timestamp, method, path, body) {
    const bodyHash = crypto.sha256(body, 'hex');
    return crypto.hmac('sha256', secret, [timestamp, method, path, bodyHash].join('\n'), 'hex');
}

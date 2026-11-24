import { check } from 'k6';
import http from 'k6/http';
import { sign } from './lib/hmac.js';

const target = (__ENV.TARGET || '').replace(/\/+$/, '');
const keyID = __ENV.HMAC_KEY_ID;
const secret = __ENV.HMAC_SECRET;
const rate = Number(__ENV.RATE);
const path = '/v1/instructions';
if (!/^https?:\/\/[^/]+$/.test(target) || !keyID || !secret || !Number.isSafeInteger(rate) || rate <= 0) {
    throw new Error('Set TARGET, HMAC_KEY_ID, HMAC_SECRET and a positive integer RATE');
}

export const options = {
    scenarios: {
        instructions: {
            executor: 'constant-arrival-rate',
            rate,
            timeUnit: '1s',
            duration: __ENV.DURATION || '10s',
            preAllocatedVUs: Math.max(1, Math.ceil(rate / 4)),
            maxVUs: rate,
        },
    },
    thresholds: {
        'http_req_duration{name:instruction}': ['p(99)<1000'],
        checks: ['rate==1'],
    },
};

export default function () {
    const now = Date.now();
    const timestamp = String(Math.floor(now / 1000));
    const body = JSON.stringify({
        instruction_id: crypto.randomUUID(),
        target_reduction_kw: '12.500',
        valid_from: new Date(now).toISOString(),
        valid_until: new Date(now + 60_000).toISOString(),
    });
    const response = http.post(`${target}${path}`, body, {
        headers: {
            'Content-Type': 'application/json',
            'X-Jammy-Key-Id': keyID,
            'X-Jammy-Timestamp': timestamp,
            'X-Jammy-Signature': sign(secret, timestamp, 'POST', path, body),
        },
        tags: { name: 'instruction' },
        redirects: 0,
    });
    check(response, { 'instruction accepted': (result) => result.status === 202 });
}

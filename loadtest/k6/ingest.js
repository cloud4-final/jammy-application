import exec from 'k6/execution';
import { open as openBatch, SeekMode } from 'k6/experimental/fs';
import http from 'k6/http';
import { Counter, Rate } from 'k6/metrics';

const targets = (__ENV.TARGETS || '').split(',').map((url) => url.trim().replace(/\/+$/, ''));
const mbps = Number(__ENV.TARGET_WIRE_MBPS_PER_SERVER || 30);
const { PAYLOAD_DIR, RUN_ID, DURATION } = __ENV;
if (targets.some((url) => !/^https?:\/\/[^/]+$/.test(url)) ||
    !Number.isFinite(mbps) || mbps <= 0 || !PAYLOAD_DIR || !DURATION ||
    !/^[A-Za-z0-9_-]+$/.test(RUN_ID || '')) {
    throw new Error('Set TARGETS, DURATION, PAYLOAD_DIR and RUN_ID; wire MB/s per server must be positive');
}

const entries = JSON.parse(open(`${PAYLOAD_DIR}/index.json`));
if (!Array.isArray(entries) || entries.length === 0) throw new Error('Payload index is empty');
const packFiles = new Map();
const batches = await Promise.all(entries.map(async (entry) => {
    const packed = __ENV.PERF_RECORDS === '1' && /^pack-[a-z-]+\.bin$/.test(entry.file) &&
        Number.isSafeInteger(entry.offset) && entry.offset >= 0;
    if ((!packed && !/^batch-\d+\.bin$/.test(entry.file)) || !entry.batch_id ||
        ![entry.events, entry.wire_bytes, entry.raw_bytes].every((n) => Number.isSafeInteger(n) && n > 0)) {
        throw new Error(`Invalid payload index entry: ${entry.file}`);
    }
    if (!packFiles.has(entry.file)) {
        const opened = openBatch(`${PAYLOAD_DIR}/${entry.file}`).then(async (file) => ({
            file, size: (await file.stat()).size,
        }));
        packFiles.set(entry.file, opened);
    }
    const { file, size } = await packFiles.get(entry.file);
    if (packed ? (entry.offset + entry.wire_bytes > size) : (size !== entry.wire_bytes)) {
        throw new Error(`Wire size differs from index: ${entry.file}`);
    }
    return { ...entry, file };
}));

const meanWireBytes = batches.reduce((total, batch) => total + batch.wire_bytes, 0) / batches.length;
const secondsPerRateUnit = __ENV.PERF_RECORDS === '1' ? 10 : 1;
const rate = Math.max(1, Math.round(mbps * 1e6 * targets.length * secondsPerRateUnit / meanWireBytes));
const iterationsPerSecond = rate / secondsPerRateUnit;
const faultSeconds = Number(__ENV.PERF_FAULT_SECONDS || 0);
const concurrentSeconds = faultSeconds > 0 ? faultSeconds + 5 : 3;
const maxVUs = __ENV.K6_MAX_VUS
    ? Number(__ENV.K6_MAX_VUS)
    : Math.max(targets.length, Math.ceil(iterationsPerSecond * concurrentSeconds));
if (!Number.isSafeInteger(maxVUs) || maxVUs <= 0) {
    throw new Error('K6_MAX_VUS must be a positive integer');
}
const wireBytes = new Counter('jammy_k6_wire_bytes');
const rawBytes = new Counter('jammy_k6_raw_bytes');
const statuses = new Counter('jammy_k6_ingest_responses');
const accepted = new Rate('jammy_k6_ingest_202');
const newBatch = new Rate('jammy_k6_ingest_new');

export const options = {
    scenarios: {
        ingest: {
            executor: 'constant-arrival-rate',
            rate,
            timeUnit: `${secondsPerRateUnit}s`,
            duration: DURATION,
            preAllocatedVUs: Math.min(maxVUs, Math.max(targets.length, Math.ceil(iterationsPerSecond / 4))),
            maxVUs,
            ...(__ENV.PERF_GRACE ? { gracefulStop: __ENV.PERF_GRACE } : {}),
        },
    },
};

export default async function () {
    const iteration = exec.scenario.iterationInTest;
    const target = targets[iteration % targets.length];
    if (__ENV.PERF_RECORDS === '1' && iteration >= batches.length) exec.test.abort('Unique payload corpus exhausted');
    const batch = batches[iteration % batches.length];
    const body = new Uint8Array(batch.wire_bytes);
    await batch.file.seek(batch.offset || 0, SeekMode.Start);
    if ((await batch.file.read(body)) !== body.byteLength) throw new Error(`Short read: ${batch.file.path}`);
    const started = Date.now();
    if (__ENV.PERF_RECORDS === '1' && iteration === 0) console.log(`PERF_START ${exec.scenario.startTime}`);
    const response = http.post(`${target}/v1/batches`, body.buffer, {
        headers: { 'Content-Type': 'application/x-protobuf', 'Content-Encoding': 'zstd' },
        tags: { name: 'ingest' },
        redirects: 0,
    });
    wireBytes.add(batch.wire_bytes);
    rawBytes.add(batch.raw_bytes);
    statuses.add(1, { status: String(response.status) });
    const ok = response.status === 202;
    accepted.add(ok);
    let receipt;
    if (ok) {
        try {
            receipt = response.json();
        } catch (_) {
            // An invalid receipt counts as a failed acceptance check.
        }
    }
    const isNew = receipt?.batch_id === batch.batch_id && receipt.duplicate === false &&
        receipt.accepted === batch.events && receipt.rejected === 0;
    newBatch.add(isNew);
    if (__ENV.PERF_RECORDS === '1') {
        const validReceipt = receipt?.batch_id === batch.batch_id && Number.isSafeInteger(receipt.accepted) &&
            receipt.accepted >= 0 && receipt.accepted <= batch.events;
        console.log(`PERF_REQUEST ${JSON.stringify({ started, target, status: response.status,
            duration_ms: response.timings.duration, wire_bytes: batch.wire_bytes,
            batch_id: batch.batch_id, accepted_events: validReceipt ? receipt.accepted : null,
            new_batch: isNew })}`);
    }
}

export function handleSummary(data) {
    return { [`results/${RUN_ID}/summary.json`]: JSON.stringify(data, null, 2) + '\n' };
}

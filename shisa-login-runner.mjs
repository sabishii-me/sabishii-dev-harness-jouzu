// One Shisa device-code sign-in, hosted OUTSIDE the adapter process.
//
// The hub's config plane spawns a one-shot adapter per request and kills it as
// soon as the answer is written, so an authorization that takes minutes cannot
// live there: it would die with the process that started it. This runner is
// detached, owns the flow until a terminal state, and records only non-secret
// progress (user code, verification URL, state, error) in an operation file the
// adapter reads back. The device code, the link token and the API key never
// appear here — the runtime's own login persists the credential through pi's
// credential store in the harness's agent directory.
//
// argv: <operation-file> <runtime-root>
// env:  JOUZU_HOME / JOUZU_SHISA_PLATFORM_URL are honoured by the runtime itself.
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const [opFile, runtimeRoot] = process.argv.slice(2);
if (!opFile || !runtimeRoot) {
  process.stderr.write('shisa-login-runner: usage: shisa-login-runner.mjs <operation-file> <runtime-root>\n');
  process.exit(2);
}

function writeOp(value) {
  try {
    fs.mkdirSync(path.dirname(opFile), { recursive: true, mode: 0o700 });
    const tmp = `${opFile}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(value), { mode: 0o600 });
    fs.renameSync(tmp, opFile);
  } catch (e) {
    process.stderr.write(`shisa-login-runner: could not record operation state: ${e.message}\n`);
  }
}
function readOp() {
  try { return JSON.parse(fs.readFileSync(opFile, 'utf8')); } catch { return null; }
}
function finish(state, error) {
  writeOp({ ...(readOp() || {}), state, ...(error ? { error: String(error).slice(0, 500) } : {}), finishedAt: new Date().toISOString() });
}

const controller = new AbortController();
// Cancellation is the adapter killing this process. Record it before exiting so
// the poller never reports a still-pending operation for a dead runner.
process.on('SIGTERM', () => { controller.abort(); finish('cancelled'); process.exit(0); });
process.on('SIGINT', () => { controller.abort(); finish('cancelled'); process.exit(0); });

writeOp({ state: 'pending', pid: process.pid, startedAt: new Date().toISOString() });

try {
  const mod = (rel) => import(pathToFileURL(path.join(runtimeRoot, 'dist', rel)).href);
  const [{ resolveJouzuPaths }, { loginShisa }, { resolveShisaGatewayUrl }] = await Promise.all([
    mod('paths.js'),
    mod('shisa-link/login.js'),
    mod('shisa-link/device-flow.js'),
  ]);
  let jouzuVersion = 'unknown';
  try { jouzuVersion = JSON.parse(fs.readFileSync(path.join(runtimeRoot, 'package.json'), 'utf8')).version || 'unknown'; } catch {}

  await loginShisa({
    onDeviceCode(info) {
      // userCode is shown to the person; it is not a secret, but it still stays
      // inside this private operation file rather than any log.
      writeOp({
        ...(readOp() || {}),
        state: 'pending',
        userCode: info.userCode,
        verifyUrl: info.verificationUri,
        intervalSeconds: info.intervalSeconds,
        expiresInSeconds: info.expiresInSeconds,
        expiresAt: Date.now() + (Number(info.expiresInSeconds) || 900) * 1000,
      });
    },
    onProgress(message) { writeOp({ ...(readOp() || {}), note: String(message).slice(0, 500) }); },
    signal: controller.signal,
  }, {
    paths: resolveJouzuPaths(),
    gatewayUrl: resolveShisaGatewayUrl(),
    jouzuVersion,
  });
  finish('approved');
  process.exit(0);
} catch (e) {
  const current = readOp() || {};
  const expired = current.expiresAt && Date.now() >= current.expiresAt;
  const state = controller.signal.aborted ? 'cancelled' : expired ? 'expired' : 'failed';
  finish(state, e && e.message ? e.message : e);
  process.exit(state === 'failed' ? 1 : 0);
}

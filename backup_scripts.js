#!/usr/bin/env node
// Runs on the RDP2 runner each cycle: AES-256-encrypt the worker scripts -> private GitHub backup repo + Google Drive (mirrors RDP1's backup).
const fs = require('fs'); const https = require('https'); const { execSync } = require('child_process');
const BPAT = process.env.BPAT; const BOWNER = 'prarthanadoshi7', BREPO = 'ec2-backup';
if (!BPAT || !process.env.BPASS) { console.log('backup: missing secrets, skip'); process.exit(0); }
try {
  execSync('tar -czf /tmp/rdp2.tar.gz mirror_worker.js package.json README.md run_mirror.bat .github/workflows/mirror.yml 2>/dev/null || tar -czf /tmp/rdp2.tar.gz mirror_worker.js package.json', { shell: '/bin/bash' });
  execSync('openssl enc -aes-256-cbc -pbkdf2 -salt -in /tmp/rdp2.tar.gz -out /tmp/rdp2.enc -pass env:BPASS');
} catch (e) { console.log('backup tar/encrypt failed: ' + e.message); process.exit(0); }
const blob = fs.readFileSync('/tmp/rdp2.enc'); const b64 = blob.toString('base64');
function gh(method, p, body) { return new Promise(r => { const data = body ? JSON.stringify(body) : null; const rq = https.request({ host: 'api.github.com', path: p, method, headers: { Authorization: 'token ' + BPAT, 'User-Agent': 'bk', Accept: 'application/vnd.github+json', ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}) } }, x => { let d = ''; x.on('data', c => d += c); x.on('end', () => r({ s: x.statusCode, j: (() => { try { return JSON.parse(d); } catch (e) { return null; } })() })); }); if (data) rq.write(data); rq.end(); }); }
function post(host, path, headers, body) { return new Promise(r => { const rq = https.request({ host, path, method: 'POST', headers }, x => { let d = ''; x.on('data', c => d += c); x.on('end', () => r({ s: x.statusCode, d })); }); rq.on('error', () => r({ s: 0, d: '' })); if (body) rq.write(body); rq.end(); }); }
(async () => {
  const P = 'rdp2/rdp2-scripts-latest.tar.gz.enc';
  const cur = await gh('GET', '/repos/' + BOWNER + '/' + BREPO + '/contents/' + P);
  const sha = (cur.s === 200 && cur.j) ? cur.j.sha : undefined;
  const g = await gh('PUT', '/repos/' + BOWNER + '/' + BREPO + '/contents/' + P, { message: 'rdp2 scripts backup', content: b64, branch: 'main', ...(sha ? { sha } : {}) });
  console.log('backup -> GitHub ' + g.s);
  if (process.env.GREF) {
    const tok = await post('oauth2.googleapis.com', '/token', { 'Content-Type': 'application/x-www-form-urlencoded' }, new URLSearchParams({ client_id: process.env.GID, client_secret: process.env.GSEC, refresh_token: process.env.GREF, grant_type: 'refresh_token' }).toString());
    let acc; try { acc = JSON.parse(tok.d).access_token; } catch (e) {}
    if (acc) { const bnd = 'b'; const meta = JSON.stringify({ name: 'rdp2-scripts-latest.tar.gz.enc', parents: [process.env.GFOLDER] }); const body = Buffer.concat([Buffer.from('--' + bnd + '\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n' + meta + '\r\n--' + bnd + '\r\nContent-Type: application/octet-stream\r\n\r\n'), blob, Buffer.from('\r\n--' + bnd + '--')]); const up = await post('www.googleapis.com', '/upload/drive/v3/files?uploadType=multipart', { Authorization: 'Bearer ' + acc, 'Content-Type': 'multipart/related; boundary=' + bnd, 'Content-Length': body.length }, body); console.log('backup -> Drive ' + up.s); }
  }
})().catch(e => console.log('backup err ' + e.message));

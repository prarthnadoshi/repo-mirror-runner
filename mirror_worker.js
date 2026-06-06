#!/usr/bin/env node
// Standalone parallel GitHub mirror worker. Runs anywhere with git + node (RDP runner or EC2).
// Does intake + change-detection + git mirroring, N repos in parallel, forever. Resumable via the mirrors table.
// Tunables (env or github_repos.config): mirror_parallel (default 15), mirror_intake (off|carded|all).
const { Client } = require('pg');
const https = require('https');
const { execFile } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

// credentials come ONLY from env (this file is committed to a PUBLIC repo). The workflow feeds them from repo secrets.
const PG = { host: process.env.SB_HOST, port: Number(process.env.SB_PORT || 5432), database: process.env.SB_DB || 'postgres', user: process.env.SB_USER, password: process.env.SB_PASSWORD, ssl: { rejectUnauthorized: false } };
if (!PG.host || !PG.user || !PG.password) { console.error('FATAL: missing SB_HOST / SB_USER / SB_PASSWORD env'); process.exit(1); }
const sleep = ms => new Promise(r => setTimeout(r, ms));
const lit = s => "'" + String(s == null ? '' : s).replace(/'/g, "''") + "'";

function git(args) { return new Promise(res => { execFile('git', args, { maxBuffer: 1 << 26, timeout: 600000, windowsHide: true }, (e, so, se) => res({ code: e ? (e.code || 1) : 0, out: (so || '') + (se || '') })); }); }
function gh(method, p, pat, body) {
  return new Promise(res => {
    const data = body ? JSON.stringify(body) : null;
    const req = https.request({ host: 'api.github.com', path: p, method, headers: { Authorization: 'token ' + pat, 'User-Agent': 'mirror-worker', Accept: 'application/vnd.github+json', ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}) } }, r => { let d = ''; r.on('data', c => d += c); r.on('end', () => { let j = null; try { j = JSON.parse(d); } catch (e) {} res({ status: r.statusCode, json: j }); }); });
    req.on('error', () => res({ status: 0, json: null })); if (data) req.write(data); req.end();
  });
}
async function pool(items, n, fn) { let i = 0; const work = Array.from({ length: Math.min(n, items.length) }, async () => { while (i < items.length) { const k = i++; try { await fn(items[k]); } catch (e) {} } }); await Promise.all(work); }
const sanitize = full => full.replace(/[^A-Za-z0-9_.-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').slice(0, 90);

async function mirrorOne(pg, PAT, USER, job) {
  const source_full = job.source_full || job.source_url.replace(/^https?:\/\/github\.com\//i, '').replace(/\.git$/, '');
  const name = job.mirror_name || sanitize(source_full);
  const mirror_url = 'https://github.com/' + USER + '/' + name;
  // 1) ensure the private repo exists (for new jobs AND error-retries; 422 = already there). Skipped on plain re-syncs.
  if (job.status === 'pending' || job.status === 'error') {
    const r = await gh('POST', '/user/repos', PAT, { name, private: true, description: 'Private mirror of github.com/' + source_full, has_issues: false, has_wiki: false, has_projects: false });
    if (r.status !== 201 && r.status !== 422) {
      if (r.status === 403 || r.status === 429) { await sleep(4000); return; } // secondary rate limit -> leave as-is, retried next loop (no false error, no attempt burned)
      const g = await gh('GET', '/repos/' + USER + '/' + name, PAT);
      if (g.status !== 200) { await pg.query("UPDATE github_repos.mirrors SET status='error', attempts=COALESCE(attempts,0)+1, last_error=" + lit('create ' + r.status) + " WHERE id=" + job.id); return; }
    }
  }
  // 2) note upstream pushed_at
  const g = await gh('GET', '/repos/' + source_full, PAT);
  const pushed = g.json && g.json.pushed_at;
  // 3) git clone --mirror + push heads/tags (skips GitHub's hidden PR refs); temp dir cleaned up after
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'mir-'));
  const up = 'https://github.com/' + source_full + '.git';
  const push = 'https://' + USER + ':' + PAT + '@github.com/' + USER + '/' + name + '.git';
  let ok = false, errtext = '';
  const c = await git(['clone', '--quiet', '--mirror', up, tmp]);
  if (c.code === 0) {
    const p = await git(['-C', tmp, 'push', '--prune', '--force', push, 'refs/heads/*:refs/heads/*', 'refs/tags/*:refs/tags/*']);
    ok = p.code === 0; errtext = p.out;
  } else errtext = c.out;
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch (e) {}
  // 4) record status + both timestamps
  if (ok) await pg.query("UPDATE github_repos.mirrors SET status='done', attempts=0, mirror_url=" + lit(mirror_url) + ", mirror_name=" + lit(name) + ", synced_at=now()" + (pushed ? (", upstream_pushed_at=" + lit(pushed)) : "") + ", checked_at=now(), last_error=NULL WHERE id=" + job.id);
  else await pg.query("UPDATE github_repos.mirrors SET status='error', attempts=COALESCE(attempts,0)+1, last_error=" + lit(String(errtext).slice(0, 400)) + " WHERE id=" + job.id);
  console.log((ok ? 'OK  ' : 'ERR ') + source_full + (ok ? '' : ' -> ' + String(errtext).slice(0, 120)));
}

async function checkRound(pg, PAT, N) {
  const batch = (await pg.query("SELECT id, source_full FROM github_repos.mirrors WHERE status='done' ORDER BY COALESCE(checked_at, to_timestamp(0)) ASC LIMIT 200")).rows;
  if (!batch.length) return;
  await pool(batch, N, async row => {
    const g = await gh('GET', '/repos/' + row.source_full, PAT);
    const pa = g.json && g.json.pushed_at;
    await pg.query("UPDATE github_repos.mirrors SET upstream_pushed_at=COALESCE(" + (pa ? lit(pa) + '::timestamptz' : 'NULL') + ", upstream_pushed_at), checked_at=now() WHERE id=" + row.id);
  });
  console.log('checked ' + batch.length + ' upstreams');
}

const INTAKE = "INSERT INTO github_repos.mirrors (source_url, source_full, status) SELECT r.github_url, r.repo_full, 'pending' FROM github_repos.repos r WHERE r.github_url IS NOT NULL AND r.repo_full ~ '^[^/]+/[^/]+$' AND (SELECT value FROM github_repos.config WHERE key='mirror_intake') IN ('carded','all') AND ((SELECT value FROM github_repos.config WHERE key='mirror_intake')='all' OR r.notified_at IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM github_repos.mirrors m WHERE m.source_url=r.github_url) ON CONFLICT (source_url) DO NOTHING";

async function main() {
  let pg = new Client(PG); await pg.connect();
  console.log('mirror worker started');
  let lastIntake = 0;
  while (true) {
    try {
      const cfg = (await pg.query("SELECT json_object_agg(key,value) c FROM github_repos.config")).rows[0].c || {};
      const PAT = cfg.mirror_pat, USER = cfg.mirror_user;
      const N = Math.max(1, Number(cfg.mirror_parallel || process.env.MIRROR_PARALLEL || 3)); // config wins -> tune live with one UPDATE, no restart
      if (!PAT || !USER) { console.log('missing mirror_pat/mirror_user'); await sleep(30000); continue; }
      if (Date.now() - lastIntake > 300000) { await pg.query(INTAKE); lastIntake = Date.now(); } // intake at most once / 5 min (was every loop) -> far lighter on Supabase
      const jobs = (await pg.query("SELECT id, source_url, source_full, mirror_name, status FROM github_repos.mirrors WHERE status='pending' OR (status='done' AND upstream_pushed_at IS NOT NULL AND upstream_pushed_at > COALESCE(synced_at, to_timestamp(0))) OR (status='error' AND COALESCE(attempts,0) < 5) ORDER BY (status='pending') DESC, (status='error') ASC, COALESCE(synced_at, to_timestamp(0)) ASC, id ASC LIMIT " + N)).rows;
      if (jobs.length) { console.log('syncing ' + jobs.length + ' (parallel ' + N + ') ...'); await pool(jobs, N, j => mirrorOne(pg, PAT, USER, j)); await sleep(3000); } // breathe between batches
      else { await checkRound(pg, PAT, N); await sleep(15000); }
    } catch (e) {
      console.log('loop error: ' + e.message); try { await pg.end(); } catch (_) {} await sleep(5000);
      try { pg = new Client(PG); await pg.connect(); } catch (_) {}
    }
  }
}
main().catch(e => { console.error('FATAL', e.message); process.exit(1); });

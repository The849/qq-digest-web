'use strict';

// QQ 群聊日报助手 —— 单文件后端，零外部依赖
// 流程：NapCat HTTP 上报 -> 临时缓冲 -> 定时用 DeepSeek 提炼「重点/待办」 -> 只保留提炼结果 -> 可选邮件推送

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const net = require('net');
const tls = require('tls');
const { DatabaseSync } = require('node:sqlite');

const DIR = __dirname;
const PORT = parseInt(process.env.PORT || '8090', 10);
const CONFIG_PATH = path.join(DIR, 'config.json');
const INDEX_PATH = path.join(DIR, 'index.html');

const DEFAULTS = {
  deepseekKey: '',
  deepseekModel: 'deepseek-chat',
  deepseekEndpoint: 'https://api.deepseek.com/chat/completions',
  groups: [],
  summarizeIntervalMinutes: 360,
  emailEnabled: false,
  smtpHost: '',
  smtpPort: 465,
  smtpUser: '',
  smtpPass: '',
  mailTo: '',
  dataDir: 'D:/qq-digest-data'
};

function loadConfig() {
  try {
    return Object.assign({}, DEFAULTS, JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')));
  } catch (e) {
    return Object.assign({}, DEFAULTS);
  }
}
let config = loadConfig();

function saveConfig() {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2), 'utf8');
}

// ---------- 数据目录（默认 D 盘，失败回退到程序目录） ----------
function resolveDataDir(dir) {
  const target = dir && dir.trim() ? dir.trim() : DEFAULTS.dataDir;
  try {
    fs.mkdirSync(target, { recursive: true });
    return target;
  } catch (e) {
    const fb = path.join(DIR, 'data');
    try { fs.mkdirSync(fb, { recursive: true }); } catch (e2) {}
    return fb;
  }
}
let dataDir = resolveDataDir(config.dataDir);
let DB_PATH = path.join(dataDir, 'qq_messages.db');
let REPORTS_DIR = path.join(dataDir, 'reports');
fs.mkdirSync(REPORTS_DIR, { recursive: true });

function openDb() {
  const db = new DatabaseSync(DB_PATH);
  db.exec(`
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL,
  group_name TEXT,
  user_id TEXT,
  user_name TEXT,
  content TEXT,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE TABLE IF NOT EXISTS highlights (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT,
  content TEXT,
  deadline TEXT,
  group_name TEXT,
  ts INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_highlights_ts ON highlights(ts);
CREATE TABLE IF NOT EXISTS groups (
  group_id TEXT PRIMARY KEY,
  name TEXT
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);
`);
  return db;
}
let db = openDb();

const insertMsgStmt = db.prepare(
  'INSERT INTO messages (group_id, group_name, user_id, user_name, content, ts) VALUES (?, ?, ?, ?, ?, ?)'
);
const insertHighlightStmt = db.prepare(
  'INSERT INTO highlights (type, content, deadline, group_name, ts) VALUES (?, ?, ?, ?, ?)'
);
const groupUpsertStmt = db.prepare(
  'INSERT INTO groups(group_id, name) VALUES(?, ?) ON CONFLICT(group_id) DO UPDATE SET name=excluded.name'
);
const groupLookupStmt = db.prepare('SELECT name FROM groups WHERE group_id = ?');
const metaGetStmt = db.prepare('SELECT value FROM meta WHERE key = ?');
const metaSetStmt = db.prepare(
  'INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'
);

let lastEventTs = 0;

function getMeta(key, def) {
  const r = metaGetStmt.get(key);
  return r ? r.value : def;
}
function setMeta(key, val) {
  metaSetStmt.run(key, String(val));
}

// ---------- 工具 ----------
function pad(n) { return String(n).padStart(2, '0'); }
function dateStr(d) {
  d = d || new Date();
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}
function timeStr(d) {
  d = d || new Date();
  return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
function tsToTime(ts) {
  const d = new Date(ts * 1000);
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function send(res, status, obj) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 5e6) { reject(new Error('请求体过大')); req.destroy(); }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function extractText(message) {
  if (typeof message === 'string') return message.trim();
  if (Array.isArray(message)) {
    return message
      .map((seg) => {
        if (!seg || typeof seg !== 'object') return '';
        switch (seg.type) {
          case 'text': return (seg.data && seg.data.text) || '';
          case 'image': return '[图片]';
          case 'face': return '[表情]';
          case 'at': return seg.data && seg.data.qq ? '@' + seg.data.qq : '';
          case 'reply': return '[回复]';
          case 'file': return '[文件]';
          case 'video': return '[视频]';
          default: return '';
        }
      })
      .filter(Boolean)
      .join('')
      .trim();
  }
  return '';
}

function isAllowedGroup(gid) {
  if (!Array.isArray(config.groups) || config.groups.length === 0) return true;
  return config.groups.map(String).includes(String(gid));
}

function resolveGroupName(gid, gname) {
  if (gname) {
    groupUpsertStmt.run(String(gid), String(gname));
    return String(gname);
  }
  const row = groupLookupStmt.get(String(gid));
  return row && row.name ? row.name : '';
}

// ---------- DeepSeek ----------
async function callDeepSeek(messages, opts) {
  opts = opts || {};
  const apiKey = opts.key || config.deepseekKey;
  const url = new URL(config.deepseekEndpoint);
  const mod = url.protocol === 'https:' ? https : http;
  const body = JSON.stringify({
    model: config.deepseekModel,
    messages,
    temperature: 0.2,
    max_tokens: opts.max_tokens || 4000,
    stream: false
  });
  const res = await new Promise((resolve, reject) => {
    const r = mod.request(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + apiKey,
        'Content-Length': Buffer.byteLength(body)
      }
    }, resolve);
    r.on('error', reject);
    r.end(body);
  });
  let text = '';
  for await (const chunk of res) text += chunk;
  if (res.statusCode >= 400) {
    let msg = text;
    try { msg = (JSON.parse(text).error && JSON.parse(text).error.message) || text; } catch (e) {}
    throw new Error('HTTP ' + res.statusCode + ' ' + msg);
  }
  let data;
  try { data = JSON.parse(text); } catch (e) { throw new Error('DeepSeek 响应解析失败'); }
  const content = data.choices && data.choices[0] && data.choices[0].message && data.choices[0].message.content;
  if (!content) throw new Error('DeepSeek 未返回内容');
  return content.trim();
}

// ---------- 提炼重点 ----------
function extractJsonArray(text) {
  const s = String(text || '');
  const a = s.indexOf('[');
  const b = s.lastIndexOf(']');
  if (a < 0 || b <= a) return [];
  try {
    const arr = JSON.parse(s.slice(a, b + 1));
    return Array.isArray(arr) ? arr : [];
  } catch (e) {
    return [];
  }
}

function getBufferedMessages() {
  return db
    .prepare('SELECT group_id, group_name, user_name, content, ts FROM messages ORDER BY ts ASC')
    .all();
}

function localExtract(rows) {
  const items = [];
  const kwImportant = ['通知', '公告', '重要', '注意', '提醒', '请各位', '各位同学', '各位同事', '各位老师', '截止', '变更', '暂停', '恢复'];
  const kwTodo = ['待办', '记得', '别忘', '要交', '提交', '几点', '开会', '集合', '报名', '回复', '确认', '完成', '完成前', '截止时间', 'deadline', 'ddl'];
  for (const r of rows) {
    const c = (r.content || '').trim();
    if (!c || c.length < 4) continue;
    const isTodo = kwTodo.some((k) => c.includes(k));
    const isImp = kwImportant.some((k) => c.includes(k));
    if (isTodo || isImp) {
      items.push({ type: isTodo ? '待办' : '重要', content: c, deadline: '', group_name: r.group_name || r.group_id });
    }
  }
  return items.slice(0, 30);
}

async function extractHighlights(rows) {
  if (rows.length === 0) return [];
  if (!config.deepseekKey) return localExtract(rows);

  let transcript = '';
  for (const r of rows.slice(0, 800)) {
    transcript += `[${tsToTime(r.ts)}] ${r.user_name || '匿名'}（${r.group_name || r.group_id}）: ${(r.content || '').slice(0, 300)}\n`;
  }
  const system = '你是群聊信息提炼助手。请从群聊消息中提炼值得长期保留的「重要信息」和「待办事项」，过滤闲聊、表情和无信息量内容。';
  const user = '请从以下群聊消息中提炼重点。只输出一个 JSON 数组，每项形如 {"type":"重要" 或 "待办","content":"...","deadline":"截止时间或空"}。最多 30 条，没有则输出 []。不要输出 JSON 以外的内容。\n\n消息：\n' + transcript;
  try {
    const out = await callDeepSeek([{ role: 'system', content: system }, { role: 'user', content: user }]);
    const arr = extractJsonArray(out);
    const clean = arr
      .filter((it) => it && it.content && String(it.content).trim())
      .map((it) => ({
        type: String(it.type).includes('待办') ? '待办' : '重要',
        content: String(it.content).trim(),
        deadline: it.deadline ? String(it.deadline) : ''
      }))
      .slice(0, 30);
    return clean;
  } catch (e) {
    console.error('[summarize] DeepSeek 提炼失败，回退本地规则：', e.message);
    return localExtract(rows);
  }
}

// ---------- 邮件发送（内置 SMTP，零依赖） ----------
function sendMail(host, port, user, pass, to, subject, text) {
  const p = Number(port);
  const mode = p === 465 ? 'ssl' : (p === 587 ? 'starttls' : 'plain');
  return new Promise((resolve, reject) => {
    let socket = null;
    let lineBuf = '';
    let pending = null;
    let settled = false;

    const fail = (e) => { if (!settled) { settled = true; try { socket && socket.destroy(); } catch (_) {} reject(e); } };
    const done = () => { if (!settled) { settled = true; try { socket && socket.end(); } catch (_) {} try { socket && socket.destroy(); } catch (_) {} resolve(); } };

    function onData(d) {
      lineBuf += d;
      let i;
      while ((i = lineBuf.indexOf('\r\n')) >= 0) {
        const line = lineBuf.slice(0, i);
        lineBuf = lineBuf.slice(i + 2);
        if (pending && /^\d{3} /.test(line)) {
          const cb = pending; pending = null;
          cb(parseInt(line.slice(0, 3), 10), line);
        }
      }
    }
    function onError(e) { fail(e); }
    function onClose() { fail(new Error('连接意外关闭')); }
    function attach(sock) {
      socket = sock;
      socket.setEncoding('utf8');
      socket.on('data', onData);
      socket.on('error', onError);
      socket.on('close', onClose);
    }
    function detach(sock) {
      sock.removeListener('data', onData);
      sock.removeListener('error', onError);
      sock.removeListener('close', onClose);
    }
    function cmd(line) {
      return new Promise((res) => {
        pending = (code, resp) => res({ code, resp });
        socket.write(line + '\r\n');
      });
    }
    function wait() {
      return new Promise((res) => {
        pending = (code, resp) => res({ code, resp });
      });
    }

    (async () => {
      try {
        if (mode === 'ssl') {
          attach(tls.connect({ host, port: p, servername: host, rejectUnauthorized: false }, () => {}));
        } else {
          attach(net.connect(p, host));
        }
        await wait(); // 读取服务器问候（220）
        await cmd('EHLO localhost');
        if (mode === 'starttls') {
          const r = await cmd('STARTTLS');
          if (r.code !== 220) throw new Error('STARTTLS 失败: ' + r.resp);
          const old = socket;
          detach(old);
          const secure = tls.connect({ socket: old, servername: host, rejectUnauthorized: false }, () => {});
          attach(secure);
          await wait(); // TLS 握手后的新问候（220）
          await cmd('EHLO localhost');
        }
        const a = await cmd('AUTH LOGIN');
        if (a.code !== 334) throw new Error('服务器不支持 AUTH LOGIN: ' + a.resp);
        await cmd(Buffer.from(user).toString('base64'));
        const pw = await cmd(Buffer.from(pass).toString('base64'));
        if (pw.code !== 235) throw new Error('认证失败（检查邮箱账号与授权码）: ' + pw.resp);
        await cmd('MAIL FROM:<' + user + '>');
        await cmd('RCPT TO:<' + to + '>');
        const dr = await cmd('DATA');
        if (dr.code !== 354) throw new Error('DATA 失败: ' + dr.resp);
        const headers = [
          'From: ' + user,
          'To: ' + to,
          'Subject: =?UTF-8?B?' + Buffer.from(subject, 'utf8').toString('base64') + '?=',
          'MIME-Version: 1.0',
          'Content-Type: text/plain; charset=UTF-8',
          ''
        ].join('\r\n');
        const body = headers + '\r\n' + text.replace(/\r\n/g, '\r\n').replace(/\n/g, '\r\n') + '\r\n.\r\n';
        const w = wait();
        socket.write(body);
        await w;
        await cmd('QUIT');
        done();
      } catch (e) {
        fail(e);
      }
    })();
  });
}

function buildEmailBody(items) {
  const imp = items.filter((i) => i.type === '重要');
  const todo = items.filter((i) => i.type === '待办');
  let out = '本次提炼到 ' + items.length + ' 条重点：\n\n';
  if (imp.length) {
    out += '【重要信息】\n';
    for (const it of imp) out += '- ' + it.content + '\n';
    out += '\n';
  }
  if (todo.length) {
    out += '【待办事项】\n';
    for (const it of todo) out += '- ' + it.content + (it.deadline ? '（截止：' + it.deadline + '）' : '') + '\n';
    out += '\n';
  }
  out += '—— 来自 QQ 群聊日报助手';
  return out;
}

// ---------- 总结流程 ----------
let summarizing = false;
async function runSummarize(manual) {
  if (summarizing) return { skipped: true };
  summarizing = true;
  try {
    const rows = getBufferedMessages();
    if (rows.length === 0) {
      setMeta('lastSummarizeTs', Math.floor(Date.now() / 1000));
      return { items: [], count: 0, emailed: false };
    }
    const items = await extractHighlights(rows);
    for (const it of items) {
      insertHighlightStmt.run(it.type, it.content, it.deadline || '', it.group_name || '', Math.floor(Date.now() / 1000));
    }
    // 只保留提炼结果，清空源数据缓冲
    db.exec('DELETE FROM messages');
    setMeta('lastSummarizeTs', Math.floor(Date.now() / 1000));

    let emailed = false;
    if (items.length && config.emailEnabled && config.smtpHost && config.smtpUser && config.mailTo) {
      try {
        const subject = '【QQ群聊摘要】' + dateStr() + ' ' + timeStr() + ' 新增 ' + items.length + ' 条重点';
        await sendMail(config.smtpHost, config.smtpPort, config.smtpUser, config.smtpPass, config.mailTo, subject, buildEmailBody(items));
        emailed = true;
      } catch (e) {
        console.error('[email] 发送失败：', e.message);
      }
    }
    console.log('[summarize] 已提炼 ' + items.length + ' 条重点，清空源缓冲' + (emailed ? '，已发邮件' : ''));
    return { items, count: items.length, emailed };
  } finally {
    summarizing = false;
  }
}

// ---------- OneBot 事件 ----------
function handleOneBotEvent(obj) {
  lastEventTs = Date.now();
  if (!obj || obj.post_type !== 'message' || obj.message_type !== 'group') return;
  const gid = String(obj.group_id);
  if (!isAllowedGroup(gid)) return;
  const content = extractText(obj.message);
  if (!content) return;
  const sender = obj.sender || {};
  const uname = sender.card || sender.nickname || '';
  const gname = resolveGroupName(gid, obj.group_name || '');
  insertMsgStmt.run(gid, gname, String(obj.user_id || ''), uname, content, Math.floor(Date.now() / 1000));
}

// ---------- 启动清理：源数据最多保留 72 小时 ----------
try {
  db.exec('DELETE FROM messages WHERE ts < ' + (Math.floor(Date.now() / 1000) - 72 * 3600));
} catch (e) {}

// ---------- HTTP 服务 ----------
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  const p = url.pathname;
  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(fs.readFileSync(INDEX_PATH, 'utf8'));
      return;
    }
    if (p === '/onebot' && req.method === 'POST') {
      try {
        handleOneBotEvent(JSON.parse(await readBody(req)));
        send(res, 200, { ok: true });
      } catch (e) {
        send(res, 400, { ok: false, error: 'JSON 解析失败' });
      }
      return;
    }
    if (p === '/api/config' && req.method === 'GET') {
      send(res, 200, {
        hasKey: !!config.deepseekKey,
        maskedKey: config.deepseekKey ? config.deepseekKey.slice(0, 4) + '****' + config.deepseekKey.slice(-4) : '',
        deepseekModel: config.deepseekModel,
        groups: config.groups,
        summarizeIntervalMinutes: config.summarizeIntervalMinutes,
        emailEnabled: config.emailEnabled,
        smtpHost: config.smtpHost,
        smtpPort: config.smtpPort,
        smtpUser: config.smtpUser,
        mailTo: config.mailTo,
        hasMailPass: !!config.smtpPass,
        dataDir: dataDir
      });
      return;
    }
    if (p === '/api/config' && req.method === 'POST') {
      const c = JSON.parse(await readBody(req));
      if (typeof c.deepseekKey === 'string' && c.deepseekKey.trim() && !c.deepseekKey.includes('****')) config.deepseekKey = c.deepseekKey.trim();
      if (typeof c.deepseekModel === 'string' && c.deepseekModel) config.deepseekModel = c.deepseekModel;
      if (Array.isArray(c.groups)) config.groups = c.groups.map((s) => String(s).trim()).filter(Boolean);
      if (typeof c.summarizeIntervalMinutes === 'number' && c.summarizeIntervalMinutes > 0) config.summarizeIntervalMinutes = Math.floor(c.summarizeIntervalMinutes);
      if (typeof c.emailEnabled === 'boolean') config.emailEnabled = c.emailEnabled;
      if (typeof c.smtpHost === 'string') config.smtpHost = c.smtpHost.trim();
      if (typeof c.smtpPort !== 'undefined') config.smtpPort = parseInt(c.smtpPort, 10) || 465;
      if (typeof c.smtpUser === 'string') config.smtpUser = c.smtpUser.trim();
      if (typeof c.smtpPass === 'string' && c.smtpPass && !c.smtpPass.includes('****')) config.smtpPass = c.smtpPass;
      if (typeof c.mailTo === 'string') config.mailTo = c.mailTo.trim();
      if (typeof c.dataDir === 'string' && c.dataDir.trim() && c.dataDir.trim() !== dataDir) config.dataDir = c.dataDir.trim();
      saveConfig();
      send(res, 200, { ok: true, dataDirChanged: config.dataDir && config.dataDir.trim() !== dataDir });
      return;
    }
    if (p === '/api/test-deepseek' && req.method === 'POST') {
      let testKey = config.deepseekKey;
      try { const b = JSON.parse(await readBody(req)); if (b && b.deepseekKey && b.deepseekKey.trim()) testKey = b.deepseekKey.trim(); } catch (e) {}
      if (!testKey) { send(res, 200, { ok: false, error: '尚未填写 API Key' }); return; }
      const t0 = Date.now();
      try {
        const out = await callDeepSeek([{ role: 'user', content: '请只回复两个字：正常' }], { max_tokens: 20, key: testKey });
        send(res, 200, { ok: true, latencyMs: Date.now() - t0, reply: out.slice(0, 50) });
      } catch (e) {
        send(res, 200, { ok: false, error: e.message });
      }
      return;
    }
    if (p === '/api/test-email' && req.method === 'POST') {
      const b = JSON.parse(await readBody(req));
      const host = (b.smtpHost || config.smtpHost).trim();
      const port = b.smtpPort || config.smtpPort;
      const user = (b.smtpUser || config.smtpUser).trim();
      const pass = (b.smtpPass && !b.smtpPass.includes('****')) ? b.smtpPass : config.smtpPass;
      const to = (b.mailTo || config.mailTo).trim();
      if (!host || !user || !pass || !to) { send(res, 200, { ok: false, error: '请完整填写 SMTP 服务器、账号、授权码和收件邮箱' }); return; }
      const t0 = Date.now();
      try {
        await sendMail(host, port, user, pass, to, '【QQ群聊日报助手】测试邮件', '这是一封测试邮件，说明邮件推送配置成功。\n\n—— QQ 群聊日报助手');
        send(res, 200, { ok: true, latencyMs: Date.now() - t0 });
      } catch (e) {
        send(res, 200, { ok: false, error: e.message });
      }
      return;
    }
    if (p === '/api/status' && req.method === 'GET') {
      const buffered = db.prepare('SELECT COUNT(*) c FROM messages').get().c;
      const highlights = db.prepare('SELECT COUNT(*) c FROM highlights').get().c;
      const lastSummarizeTs = parseInt(getMeta('lastSummarizeTs', '0'), 10);
      const interval = config.summarizeIntervalMinutes * 60 * 1000;
      const nextAt = lastSummarizeTs ? lastSummarizeTs * 1000 + interval : Date.now();
      send(res, 200, {
        napcatConnected: Date.now() - lastEventTs < 10 * 60 * 1000,
        bufferedCount: buffered,
        highlightCount: highlights,
        hasKey: !!config.deepseekKey,
        emailEnabled: config.emailEnabled,
        summarizeIntervalMinutes: config.summarizeIntervalMinutes,
        lastSummarizeTs: lastSummarizeTs || null,
        nextSummarizeAt: nextAt,
        onebotUrl: 'http://127.0.0.1:' + PORT + '/onebot',
        dataDir: dataDir
      });
      return;
    }
    if (p === '/api/summarize' && req.method === 'POST') {
      const r = await runSummarize(true);
      if (r.skipped) { send(res, 200, { ok: false, error: '正在总结中' }); return; }
      send(res, 200, { ok: true, count: r.count, emailed: r.emailed });
      return;
    }
    if (p === '/api/highlights' && req.method === 'GET') {
      const rows = db.prepare('SELECT id, type, content, deadline, group_name, ts FROM highlights ORDER BY ts DESC LIMIT 500').all();
      send(res, 200, rows.map((r) => ({
        id: r.id, type: r.type, content: r.content, deadline: r.deadline,
        group: r.group_name || '', time: tsToTime(r.ts)
      })));
      return;
    }
    if (p === '/api/highlights/clear' && req.method === 'POST') {
      db.exec('DELETE FROM highlights');
      send(res, 200, { ok: true });
      return;
    }
    send(res, 404, { ok: false, error: 'not found' });
  } catch (e) {
    console.error('[server]', e);
    try { send(res, 500, { ok: false, error: e.message }); } catch (e2) {}
  }
});

// ---------- 定时总结 ----------
setInterval(() => {
  try {
    const lastTs = parseInt(getMeta('lastSummarizeTs', '0'), 10);
    const intervalMs = config.summarizeIntervalMinutes * 60 * 1000;
    const buffered = db.prepare('SELECT COUNT(*) c FROM messages').get().c;
    if (buffered > 0 && Date.now() - lastTs * 1000 >= intervalMs) {
      runSummarize(false).catch((e) => console.error('[scheduler]', e.message));
    }
  } catch (e) {
    console.error('[scheduler]', e.message);
  }
}, 30 * 1000);

server.listen(PORT, '127.0.0.1', () => {
  console.log('==============================================');
  console.log('  QQ 群聊日报助手 已启动');
  console.log('  网页界面：http://127.0.0.1:' + PORT);
  console.log('  NapCat 上报地址：http://127.0.0.1:' + PORT + '/onebot');
  console.log('  数据目录：' + dataDir);
  console.log('==============================================');
  if (!process.env.NO_OPEN) {
    try { require('child_process').exec('start "" http://127.0.0.1:' + PORT, () => {}); } catch (e) {}
  }
});

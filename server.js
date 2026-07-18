// ─── Veridy — proxy WebSocket persistant pour le tuteur vocal ─────────────────
// Navigateur ↔ (ce proxy) ↔ OpenAI Realtime. Le proxy détient la session OpenAI
// (clé API jamais exposée au navigateur), COMPTE les secondes réellement connectées
// et COUPE quand les crédits/plafond sont atteints → métrage inviolable côté serveur.
//
// Auth : ticket HMAC signé par Veridy (secret partagé). Le proxy récupère les
// instructions du cours + décrémente les crédits via l'API interne de Veridy.
//
// Env requis :
//   PORT                        (défaut 8787)
//   OPENAI_API_KEY              clé OpenAI (reste sur le VPS, jamais côté navigateur)
//   VERIDY_VOICE_PROXY_SECRET   secret partagé Veridy ↔ proxy (signature ticket + API interne)
//   VERIDY_API_URL              ex. https://veridy.vercel.app
// Optionnel :
//   OPENAI_REALTIME_URL         défaut wss://api.openai.com/v1/realtime
//   CONSUME_EVERY_S             cadence de décompte (défaut 15 s)

import http from 'http';
import crypto from 'crypto';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = Number(process.env.PORT || 8787);
const SECRET = process.env.VERIDY_VOICE_PROXY_SECRET || '';
const VERIDY_API = (process.env.VERIDY_API_URL || '').replace(/\/$/, '');
const OPENAI_URL = process.env.OPENAI_REALTIME_URL || 'wss://api.openai.com/v1/realtime';
const CONSUME_EVERY_S = Number(process.env.CONSUME_EVERY_S || 15);

if (!SECRET || !VERIDY_API) {
  console.error('Config manquante : VERIDY_VOICE_PROXY_SECRET et VERIDY_API_URL requis.');
  process.exit(1);
}

// Clé OpenAI : soit fournie en env (OPENAI_API_KEY), soit récupérée depuis Veridy (canal interne
// protégé par secret) — évite de stocker la clé sur le VPS. Mise en cache après le 1er appel.
let cachedOpenAIKey = process.env.OPENAI_API_KEY || '';
async function getOpenAIKey() {
  if (cachedOpenAIKey) return cachedOpenAIKey;
  const r = await fetch(`${VERIDY_API}/api/internal/openai-key`, {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'x-proxy-secret': SECRET }, body: '{}',
  });
  if (!r.ok) throw new Error(`openai-key ${r.status}`);
  const d = await r.json();
  if (!d.key) throw new Error('openai-key vide');
  cachedOpenAIKey = d.key;
  return cachedOpenAIKey;
}

// Vérifie un ticket "<payload_b64url>.<hmac_b64url>" signé par Veridy. Retourne le payload ou null.
function verifyTicket(t) {
  const [p, s] = String(t || '').split('.');
  if (!p || !s) return null;
  const sig = crypto.createHmac('sha256', SECRET).update(p).digest('base64url');
  const a = Buffer.from(sig), b = Buffer.from(s);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  let payload;
  try { payload = JSON.parse(Buffer.from(p, 'base64url').toString('utf8')); } catch { return null; }
  if (!payload || !payload.exp || Date.now() / 1000 > payload.exp) return null;
  return payload; // { c: courseId, b: buyer, s: maxSeconds, lang, mod? }
}

async function veridyPost(path, body) {
  const r = await fetch(`${VERIDY_API}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-proxy-secret': SECRET },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`veridy ${path} → ${r.status}`);
  return r.json();
}

const server = http.createServer((req, res) => { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('veridy voice-proxy ok'); });
const wss = new WebSocketServer({ server, path: '/rt' });

wss.on('connection', async (client, req) => {
  const ticket = verifyTicket(new URL(req.url, 'http://x').searchParams.get('ticket'));
  if (!ticket) { client.close(4001, 'ticket invalide'); return; }
  const { c: courseId, b: buyer, s: maxSeconds, lang, mod } = ticket;

  let cfg, OPENAI_KEY;
  try { cfg = await veridyPost('/api/internal/voice-config', { courseId, lang: lang || 'fr', module: mod ?? null }); }
  catch { client.close(4002, 'config indisponible'); return; }
  try { OPENAI_KEY = await getOpenAIKey(); }
  catch (e) { console.error('[openai-key]', String(e).slice(0, 120)); client.close(4004, 'cle openai indisponible'); return; }

  const started = Date.now();
  let consumed = 0, closed = false;
  const elapsed = () => Math.round((Date.now() - started) / 1000);
  const flush = async (extra = 0) => { const total = elapsed(); const delta = total - consumed + extra; consumed = total; if (delta > 0 && buyer) { try { await veridyPost('/api/internal/voice-consume', { courseId, buyer, seconds: delta }); } catch { /* best-effort */ } } };

  const oai = new WebSocket(`${OPENAI_URL}?model=${encodeURIComponent(cfg.model)}`, { headers: { Authorization: `Bearer ${OPENAI_KEY}`, 'OpenAI-Beta': 'realtime=v1' } });

  const shutdown = (code, msg) => {
    if (closed) return; closed = true;
    clearInterval(timer);
    flush();
    try { oai.close(); } catch { /* ignore */ }
    try { client.close(code || 1000, msg || 'fin'); } catch { /* ignore */ }
  };

  const timer = setInterval(() => {
    const total = elapsed();
    if (total - consumed >= CONSUME_EVERY_S) flush();
    if (total >= maxSeconds) shutdown(4003, 'credits epuises');
  }, 2000);

  oai.on('open', () => {
    // Config de session GA : instructions + audio pcm16 + VAD serveur. (Ajuster ici si l'API évolue.)
    oai.send(JSON.stringify({
      type: 'session.update',
      session: {
        instructions: cfg.instructions,
        audio: {
          input: { format: 'pcm16', transcription: { model: 'whisper-1' }, turn_detection: { type: 'server_vad' } },
          output: { format: 'pcm16', voice: cfg.voice },
        },
      },
    }));
    if (client.readyState === 1) client.send(JSON.stringify({ t: 'ready', maxSeconds }));
  });

  const DEBUG = process.env.PROXY_DEBUG === '1';
  const seenTypes = new Set();
  let audioDeltas = 0;
  oai.on('message', (data) => {
    let ev; try { ev = JSON.parse(data.toString()); } catch { return; }
    // DEBUG : logue chaque type d'event OpenAI rencontré une fois (révèle le vrai protocole GA au 1er test).
    if (DEBUG && ev.type && !seenTypes.has(ev.type)) { seenTypes.add(ev.type); console.log('[oai event]', ev.type); }
    // Audio sortant (base64 pcm16) → binaire vers le navigateur. Tolère plusieurs noms d'events GA.
    const isAudioDelta = /(^|\.)(output_)?audio\.delta$/.test(ev.type || '') || (ev.type === 'response.audio.delta');
    const delta = ev.delta || ev.audio;
    if (isAudioDelta && typeof delta === 'string') {
      audioDeltas++; if (DEBUG && audioDeltas === 1) console.log('[oai] premier chunk audio reçu ✓');
      if (client.readyState === 1) client.send(Buffer.from(delta, 'base64'));
    } else if (/audio_transcript\.delta$/.test(ev.type || '') && ev.delta) {
      if (client.readyState === 1) client.send(JSON.stringify({ t: 'transcript', delta: ev.delta }));
    } else if (ev.type === 'error' || ev.type === 'response.error') {
      console.error('[oai error]', JSON.stringify(ev.error || ev).slice(0, 400));
    } else if (ev.type === 'session.created' || ev.type === 'session.updated') {
      if (DEBUG) console.log('[oai]', ev.type, 'ok');
    }
  });
  oai.on('close', () => shutdown(1000, 'oai fermé'));
  oai.on('error', (e) => { console.error('[oai ws]', String(e).slice(0, 200)); shutdown(1011, 'oai erreur'); });

  // Audio entrant du navigateur (binaire pcm16 24kHz mono) → OpenAI (base64).
  client.on('message', (data, isBinary) => {
    if (!isBinary || oai.readyState !== 1) return;
    oai.send(JSON.stringify({ type: 'input_audio_buffer.append', audio: Buffer.from(data).toString('base64') }));
  });
  client.on('close', () => shutdown(1000, 'client fermé'));
  client.on('error', () => shutdown(1011, 'client erreur'));
});

server.listen(PORT, () => console.log(`veridy voice-proxy écoute sur :${PORT} (ws path /rt)`));

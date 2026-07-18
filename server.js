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

  // API GA : PAS de header OpenAI-Beta (rejeté « beta_api_shape_disabled »).
  const oai = new WebSocket(`${OPENAI_URL}?model=${encodeURIComponent(cfg.model)}`, { headers: { Authorization: `Bearer ${OPENAI_KEY}` } });

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

  // Outil RAG : le modèle appelle search_course(query) → on interroge Veridy (recherche
  // sémantique dans le cours) → on renvoie les extraits. Donne au tuteur VOCAL la même
  // profondeur que le tuteur écrit (au-delà du résumé d'instructions, cap 12k).
  const searchTool = {
    type: 'function',
    name: 'search_course',
    description: 'Recherche sémantique dans le contenu complet du cours. Renvoie les extraits les plus pertinents. À appeler AVANT de répondre à toute question portant sur le contenu du cours.',
    parameters: {
      type: 'object',
      properties: { query: { type: 'string', description: 'la question ou les mots-clés à rechercher dans le cours' } },
      required: ['query'],
    },
  };
  const toolDirective = "\n\nOUTIL: tu disposes de search_course(query) qui renvoie des extraits du cours. Pour TOUTE question sur le contenu, appelle d'abord search_course, puis fonde ta réponse UNIQUEMENT sur les extraits renvoyés. Ne lis pas les repères [n] à voix haute. Si aucun extrait pertinent, dis-le simplement.";

  oai.on('open', () => {
    // Config de session GA : session.type='realtime', formats audio en objet {type:'audio/pcm',rate:24000}.
    oai.send(JSON.stringify({
      type: 'session.update',
      session: {
        type: 'realtime',
        instructions: cfg.instructions + toolDirective,
        tools: [searchTool],
        tool_choice: 'auto',
        audio: {
          input: { format: { type: 'audio/pcm', rate: 24000 }, turn_detection: { type: 'server_vad' } },
          output: { format: { type: 'audio/pcm', rate: 24000 }, voice: cfg.voice },
        },
      },
    }));
    if (client.readyState === 1) client.send(JSON.stringify({ t: 'ready', maxSeconds }));
  });

  const DEBUG = process.env.PROXY_DEBUG === '1';
  const seenTypes = new Set();
  let audioDeltas = 0;

  // Exécute un appel d'outil du modèle : search_course → Veridy RAG → renvoie les extraits + relance une réponse.
  // Protocole GA (validé) : event response.function_call_arguments.done {call_id,name,arguments} →
  // conversation.item.create {type:'function_call_output',call_id,output} → response.create.
  async function handleToolCall(ev) {
    let query = '';
    try { query = String(JSON.parse(ev.arguments || '{}').query || '').trim(); } catch { /* args malformés */ }
    let passages = [];
    if (query) {
      try { const r = await veridyPost('/api/internal/course-search', { courseId, query, k: 6 }); passages = Array.isArray(r.passages) ? r.passages : []; }
      catch (e) { console.error('[course-search]', String(e).slice(0, 120)); }
    }
    const output = passages.length
      ? passages.map((p, i) => `[${i + 1}] ${p}`).join('\n\n')
      : 'Aucun extrait pertinent trouvé dans le cours.';
    if (DEBUG) console.log(`[tool] search_course(${JSON.stringify(query)}) → ${passages.length} extrait(s)`);
    if (oai.readyState === 1) {
      oai.send(JSON.stringify({ type: 'conversation.item.create', item: { type: 'function_call_output', call_id: ev.call_id, output } }));
      oai.send(JSON.stringify({ type: 'response.create' }));
    }
  }

  oai.on('message', (data) => {
    let ev; try { ev = JSON.parse(data.toString()); } catch { return; }
    // DEBUG : logue chaque type d'event OpenAI rencontré une fois (révèle le vrai protocole GA au 1er test).
    if (DEBUG && ev.type && !seenTypes.has(ev.type)) { seenTypes.add(ev.type); console.log('[oai event]', ev.type); }
    // Appel d'outil terminé → exécuter le RAG et renvoyer le résultat au modèle.
    if (ev.type === 'response.function_call_arguments.done' && ev.name === 'search_course') {
      handleToolCall(ev).catch((e) => console.error('[tool]', String(e).slice(0, 150)));
      return;
    }
    // GA : audio sortant = response.output_audio.delta (base64 pcm16) → binaire vers le navigateur.
    const delta = ev.delta || ev.audio;
    if ((ev.type === 'response.output_audio.delta' || ev.type === 'response.audio.delta') && typeof delta === 'string') {
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

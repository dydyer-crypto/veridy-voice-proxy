# Veridy voice-proxy (tuteur vocal, métrage inviolable)

Proxy WebSocket persistant à héberger **hors Vercel** (sur ton VPS KVM). Il relaie
le tuteur vocal `navigateur ↔ OpenAI Realtime`, **compte les secondes réellement
connectées** et **coupe** quand les crédits sont épuisés — le navigateur ne peut
plus sous-déclarer. La clé OpenAI reste sur le VPS.

```
Navigateur (PCM16 24kHz)
   │  wss://voice.tondomaine.com/rt?ticket=<signé par Veridy>
   ▼
voice-proxy (ce service, KVM2)  ──►  Veridy /api/internal/voice-config  (instructions du cours)
   │                             ──►  Veridy /api/internal/voice-consume (décompte crédits)
   ▼
OpenAI Realtime (WS, clé API serveur)
```

## Prérequis
- Un (sous-)domaine pointant sur le KVM2 (ex. `voice.tondomaine.com`), pour le TLS/wss.
- Docker + docker-compose **ou** Node 20+.
- La **même** valeur de `VERIDY_VOICE_PROXY_SECRET` ici ET dans les variables d'env Veridy (Vercel).

## Déploiement (Docker, recommandé)
```bash
# 1. Récupérer le dossier voice-proxy/ sur le VPS (git clone du repo veridy, ou scp).
cd voice-proxy

# 2. Créer le .env (NE PAS committer)
cat > .env <<EOF
OPENAI_API_KEY=sk-...            # ta clé OpenAI
VERIDY_VOICE_PROXY_SECRET=<un secret long aléatoire, identique côté Veridy>
VERIDY_API_URL=https://veridy.vercel.app
EOF

# 3. Lancer
docker compose up -d --build
docker compose logs -f            # doit afficher "voice-proxy écoute sur :8787"
curl http://127.0.0.1:8787         # -> "veridy voice-proxy ok"
```

## TLS / wss (nginx + certbot)
```bash
sudo cp nginx-voice-proxy.conf /etc/nginx/sites-available/voice-proxy
# éditer le server_name (voice.tondomaine.com), puis :
sudo ln -s /etc/nginx/sites-available/voice-proxy /etc/nginx/sites-enabled/
sudo certbot --nginx -d voice.tondomaine.com
sudo nginx -t && sudo systemctl reload nginx
```

## Alternative sans Docker (Node + systemd)
```bash
npm install --omit=dev
# service systemd /etc/systemd/system/veridy-voice-proxy.service :
#   [Service]
#   WorkingDirectory=/opt/voice-proxy
#   EnvironmentFile=/opt/voice-proxy/.env
#   ExecStart=/usr/bin/node server.js
#   Restart=always
sudo systemctl enable --now veridy-voice-proxy
```

## Côté Veridy (Vercel) — activer le mode proxy
Ajouter ces variables d'env (Production) :
```
VERIDY_VOICE_PROXY_URL   = wss://voice.tondomaine.com/rt
VERIDY_VOICE_PROXY_SECRET = <le MÊME secret que sur le VPS>
```
Tant que `VERIDY_VOICE_PROXY_URL` n'est pas défini, Veridy reste sur le mode WebRTC direct (aucune régression).

## Vérification
1. `curl https://voice.tondomaine.com/` → `veridy voice-proxy ok`.
2. Dans Veridy (connecté avec crédits), « Parler au tuteur » : le widget passe en mode proxy (wss) au lieu de WebRTC direct.
3. Les crédits diminuent **côté serveur** au fil de la session ; à 0, la session se coupe automatiquement.

> Note : les noms d'événements/format audio de l'API Realtime GA peuvent nécessiter un ajustement dans `server.js` (bloc `session.update` et `oai.on('message')`) selon la version OpenAI courante — c'est le seul point à valider au premier test réel.

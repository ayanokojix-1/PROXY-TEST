// server.js — supports CONNECT tunnels + path proxy + WebSocket upgrade
import express from 'express'
import http from 'http'
import url from 'url'
import net from 'net'
import { WebSocketServer, WebSocket } from 'ws'
import fetch from 'node-fetch' // ensure installed
import { pipeline } from 'stream'
import { promisify } from 'util'

const pipe = promisify(pipeline)

const app = express()
const server = http.createServer(app)
// wss will be used for upgrade handling (we'll call handleUpgrade manually)
const wss = new WebSocketServer({ noServer: true })

const WA_ORIGIN = 'https://web.whatsapp.com'
const DEFAULT_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36'

// If you want to restrict who can use /proxy, set WA_PROXY_PUBLIC=false in env
const PROXY_PUBLIC = process.env.WA_PROXY_PUBLIC === 'true'
const PROXY_KEY = process.env.WA_PROXY_KEY || 'NEXUS'

// Debug helper
const log = (...args) => console.log(new Date().toISOString(), ...args)

// --- Upgrade handler (must be registered on server) ---
server.on('upgrade', (req, socket, head) => {
  try {
    const pathname = url.parse(req.url).pathname
    log('[UPGRADE]', pathname, 'UpgradeHeader=', req.headers['upgrade'])
    // Only allow the WebSocket upgrade for our expected path
    if (pathname === '/wa-proxy') {
      // let wss handle the rest
      wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req))
    } else {
      // not allowed - close socket
      log('[UPGRADE] unknown path -> destroy socket', pathname)
      try { socket.write('HTTP/1.1 404 Not Found\r\n\r\n') } catch (e) {}
      socket.destroy()
    }
  } catch (err) {
    log('[UPGRADE ERROR]', err)
    try { socket.destroy() } catch {}
  }
})

// --- WebSocket connection handler: forward to official WhatsApp WS ---
wss.on('connection', (client, req) => {
  log('[WS] client connected, forwarding to WhatsApp')

  // Target WhatsApp websocket endpoint (Baileys expects /ws/chat)
  const TARGET_WS = 'wss://web.whatsapp.com/ws/chat'

  // Preserve client's subprotocols if any (sec-websocket-protocol)
  const clientProtocol = req.headers['sec-websocket-protocol'] || undefined

  // Build headers to forward — keep necessary ones and set origin/host
  const forwardedHeaders = {}
  for (const [k, v] of Object.entries(req.headers)) {
    const lk = k.toLowerCase()
    // Skip hop-by-hop headers and ws-specific ones we'll set manually
    if (['connection', 'upgrade', 'sec-websocket-key', 'sec-websocket-accept', 'sec-websocket-protocol'].includes(lk)) continue
    forwardedHeaders[k] = v
  }
  forwardedHeaders['host'] = 'web.whatsapp.com'
  forwardedHeaders['origin'] = WA_ORIGIN
  forwardedHeaders['referer'] = WA_ORIGIN + '/'
  forwardedHeaders['user-agent'] = forwardedHeaders['user-agent'] || DEFAULT_USER_AGENT

  // Create target socket connection to WhatsApp
  const targetSocket = new WebSocket(TARGET_WS, clientProtocol ? [clientProtocol] : undefined, {
    headers: forwardedHeaders,
    // Baileys/WhatsApp don't like aggressive permessage-deflate in some proxies — disable it.
     rejectUnauthorized: false,
    perMessageDeflate: false,
    // Short handshake timeout to fail fast
    handshakeTimeout: 20000
  })

  let open = false

  // Data from client -> forward to target (only when target open)
  client.on('message', data => {
    if (!open) return
    try { targetSocket.send(data) } catch (e) { log('[WS] send->target failed', e.message) }
  })

  client.on('close', (code, reason) => {
    log('[WS] client closed', code, reason?.toString())
    try { targetSocket.close(code || 1000, reason?.toString()) } catch (e) { targetSocket.terminate() }
  })
  client.on('error', err => {
    log('[WS] client error', err.message)
    try { targetSocket.terminate() } catch {}
  })

  // Target socket events
  targetSocket.on('open', () => {
    open = true
    log('[WS] connected to WhatsApp target')
    targetSocket.on('message', d => {
      try { client.send(d) } catch (e) { log('[WS] send->client failed', e.message) }
    })
  })

  targetSocket.on('close', (code, reason) => {
    log('[WS] target closed', code, reason?.toString())
    try { client.terminate() } catch {}
  })

  targetSocket.on('error', err => {
    log('[WS] target error', err.message)
    try { client.terminate() } catch {}
  })
})

// --- CONNECT handler for HTTPS tunneling (used by HttpsProxyAgent/CONNECT flows) ---
server.on('connect', (req, clientSocket, head) => {
  // req.url looks like: "mmg.whatsapp.net:443"
  const [host, portStr] = req.url.split(':')
  const port = parseInt(portStr || '443', 10) || 443
  log('[CONNECT]', req.url)

  const serverSocket = net.connect(port, host, () => {
    try {
      clientSocket.write('HTTP/1.1 200 Connection Established\r\n' +
        'Proxy-agent: node-ws-proxy\r\n' +
        '\r\n')
      if (head && head.length) serverSocket.write(head)
      serverSocket.pipe(clientSocket)
      clientSocket.pipe(serverSocket)
    } catch (e) {
      log('[CONNECT] write error', e.message)
      try { clientSocket.end() } catch {}
    }
  })

  serverSocket.on('error', (err) => {
    log('[CONNECT ERROR]', err.message)
    try { clientSocket.write('HTTP/1.1 502 Bad Gateway\r\n\r\n') } catch {}
    clientSocket.end()
  })

  clientSocket.on('error', (err) => {
    log('[CONNECT] clientSocket error', err.message)
    serverSocket.end()
  })
})

// --- path-based proxy endpoint: /proxy?url=<url-encoded-target> ---
// supports GET/HEAD/POST/etc and streams body
// Optional protection: require API key unless PROXY_PUBLIC=true
app.all('/proxy', async (req, res) => {
  try {
    const target = req.query.url || req.headers['x-target-url']
    if (!target) return res.status(400).send('Missing ?url=...')

    if (!PROXY_PUBLIC) {
      const clientKey = req.headers['x-wa-proxy-key'] || req.query.key
      if (clientKey !== PROXY_KEY) {
        return res.status(401).send('Unauthorized')
      }
    }

    const method = req.method
    // copy headers but remove hop-by-hop headers and host — we'll set Host specially
    const outgoingHeaders = {}
    for (const [k, v] of Object.entries(req.headers)) {
      const lk = k.toLowerCase()
      if (['host', 'connection', 'keep-alive', 'proxy-authorization', 'proxy-authenticate', 'upgrade', 'te', 'transfer-encoding'].includes(lk)) continue
      outgoingHeaders[k] = v
    }

    // Force WhatsApp-like headers for media endpoints
    outgoingHeaders['origin'] = WA_ORIGIN
    outgoingHeaders['referer'] = WA_ORIGIN + '/'
    outgoingHeaders['user-agent'] = outgoingHeaders['user-agent'] || DEFAULT_USER_AGENT
    try {
      const targetHostname = new URL(target).hostname
      outgoingHeaders['host'] = targetHostname
    } catch (e) {}

    // When forwarding body, pass the original req stream for POST/PUT/PATCH
    const fetchOptions = {
      method,
      headers: outgoingHeaders,
      body: (method === 'GET' || method === 'HEAD') ? undefined : req,
      redirect: 'follow'
    }

    log('[PROXY FORWARD]', method, target)
    const upstream = await fetch(target, fetchOptions)

    // Copy response status and headers (filter hop-by-hop)
    res.status(upstream.status)
    upstream.headers.forEach((value, name) => {
      const ln = name.toLowerCase()
      if (['transfer-encoding', 'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'upgrade'].includes(ln)) return
      res.setHeader(name, value)
    })

    if (upstream.body) {
      await pipe(upstream.body, res)
    } else {
      res.end()
    }
  } catch (err) {
    log('[PROXY ERROR]', err)
    if (!res.headersSent) res.status(500).json({ error: err.message })
    else res.end()
  }
})

// --- Prevent Express from accidentally replying 405 to plain HTTP GETs to /wa-proxy ---
// If someone tries to visit https://host/wa-proxy via plain HTTP (no upgrade), return 426 (Upgrade Required)
// This must be before other routes so it doesn't get swallowed
app.use('/wa-proxy', (req, res, next) => {
  // If this was a WebSocket upgrade, it shouldn't reach Express (upgrade handler handles it).
  // But sometimes reverse proxies transform requests into plain HTTP — so detect that and respond clearly.
  const isUpgrade = (req.headers['upgrade'] || '').toLowerCase() === 'websocket'
  if (!isUpgrade) {
    return res.status(426).send('Upgrade Required: please use WebSocket (wss://) to connect to this endpoint')
  }
  next()
})

// --- simple health endpoint ---
app.get('/health', (req, res) => res.json({ ok: true, time: new Date().toISOString() }))

// root info
app.get('/', (req, res) => {
  res.json({
    ok: true,
    info: 'WhatsApp proxy',
    ws: '/wa-proxy (wss://)',
    proxy: '/proxy?url=<encoded target>',
    notes: PROXY_PUBLIC ? 'proxy public' : 'proxy protected'
  })
})

// set some server timeouts to accommodate uploads
server.keepAliveTimeout = 120000
server.headersTimeout = 120000

const PORT = process.env.PORT || 3000
server.listen(PORT, () => log(`WhatsApp proxy listening on :${PORT}`))

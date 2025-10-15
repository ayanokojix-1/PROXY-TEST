// server.js — supports CONNECT tunnels + path proxy + WebSocket upgrade
import express from 'express'
import http from 'http'
import url from 'url'
import net from 'net'
import { WebSocketServer, WebSocket } from 'ws'
import fetch from 'node-fetch' // make sure node-fetch is installed
import { pipeline } from 'stream'
import { promisify } from 'util'

const pipe = promisify(pipeline)

const app = express()
const server = http.createServer(app)
const wss = new WebSocketServer({ noServer: true })

const WA_ORIGIN = 'https://web.whatsapp.com'
const DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36'
const PROXY_PUBLIC = process.env.WA_PROXY_PUBLIC === 'false' // optional: if false restrict /proxy usage (see notes)

// ---- websocket upgrade for /wa-proxy (WhatsApp Web Socket) ----
server.on('upgrade', (req, socket, head) => {
  const pathname = url.parse(req.url).pathname
  if (pathname === '/wa-proxy') {
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req))
  } else {
    socket.destroy()
  }
})

wss.on('connection', (client, req) => {
  const TARGET_WS = 'wss://web.whatsapp.com/ws/chat'
  const targetSocket = new WebSocket(TARGET_WS, {
    headers: { ...req.headers, host: 'web.whatsapp.com', origin: WA_ORIGIN }
  })

  let open = false
  client.on('message', data => { if (open) targetSocket.send(data) })
  client.on('close', (code, reason) => {
    try { targetSocket.close(code || 1000, reason?.toString()) } catch (e) { targetSocket.terminate() }
  })
  client.on('error', () => targetSocket.terminate())

  targetSocket.on('open', () => {
    open = true
    targetSocket.on('message', d => client.send(d))
  })
  targetSocket.on('close', () => client.terminate())
  targetSocket.on('error', () => client.terminate())
})

// ---- support CONNECT method -> allow HTTPS tunneling (used by HttpsProxyAgent) ----
server.on('connect', (req, clientSocket, head) => {
  // req.url looks like: "mmg.whatsapp.net:443"
  const [host, portStr] = req.url.split(':')
  const port = parseInt(portStr || '443', 10) || 443
  console.log('[CONNECT]', req.url)

  const serverSocket = net.connect(port, host, () => {
    clientSocket.write('HTTP/1.1 200 Connection Established\r\n' +
      'Proxy-agent: node-ws-proxy\r\n' +
      '\r\n')
    // push any buffered data
    if (head && head.length) serverSocket.write(head)
    // bi-directional piping
    serverSocket.pipe(clientSocket)
    clientSocket.pipe(serverSocket)
  })

  serverSocket.on('error', (err) => {
    console.error('[CONNECT ERROR]', err.message)
    try { clientSocket.write('HTTP/1.1 502 Bad Gateway\r\n\r\n') } catch {}
    clientSocket.end()
  })

  clientSocket.on('error', (err) => {
    serverSocket.end()
  })
})

// ---- path-based proxy endpoint: /proxy?url=<url-encoded-target>
// This lets the bot call fetch('https://your-proxy/proxy?url=https%3A%2F%2Fmmg.whatsapp.net%2F...')
// and we can set required headers (origin/referer/user-agent), forward body, stream the response.
app.all('/proxy', async (req, res) => {
  try {
    const target = req.query.url || req.headers['x-target-url']
    if (!target) return res.status(400).send('Missing ?url=...')

    // Optional protection: require an API key unless PROXY_PUBLIC=true
// Optional protection: require an API key unless PROXY_PUBLIC=true
if (!PROXY_PUBLIC) {
  const clientKey = req.headers['x-wa-proxy-key'] || req.query.key
  if (clientKey !== 'NEXUS') {
    return res.status(401).send('Unauthorized')
  }
}

    const method = req.method
    // Copy headers but remove hop-by-hop headers and host — we'll set Host specially
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
    // let the real host header reflect the target
    try {
      const targetHostname = new URL(target).hostname
      outgoingHeaders['host'] = targetHostname
    } catch (e) {}

    // When forwarding body, pass the original req stream for POST/PUT/PATCH
    const fetchOptions = {
      method,
      headers: outgoingHeaders,
      // node-fetch supports a stream as body
      body: (method === 'GET' || method === 'HEAD') ? undefined : req,
      redirect: 'follow'
    }

    console.log('[PROXY FORWARD]', method, target)
    const upstream = await fetch(target, fetchOptions)

    // Copy response status and headers (filter hop-by-hop)
    res.status(upstream.status)
    upstream.headers.forEach((value, name) => {
      const ln = name.toLowerCase()
      if (['transfer-encoding', 'connection', 'keep-alive', 'proxy-authenticate', 'proxy-authorization', 'upgrade'].includes(ln)) return
      res.setHeader(name, value)
    })

    // Stream body
    if (upstream.body) {
      await pipe(upstream.body, res)
    } else {
      res.end()
    }
  } catch (err) {
    console.error('[PROXY ERROR]', err)
    if (!res.headersSent) res.status(500).json({ error: err.message })
    else res.end()
  }
})

// optional simple health endpoint
app.get('/health', (req, res) => res.json({ ok: true, time: new Date().toISOString() }))

// root info
app.get('/', (req, res) => {
  res.json({
    ok: true,
    info: 'WhatsApp media proxy',
    ws: '/wa-proxy',
    proxy: '/proxy?url=<encoded target>',
    notes: PROXY_PUBLIC ? 'proxy public' : 'proxy protected (set WA_PROXY_KEY)'
  })
})

// set some server timeouts to accommodate uploads
server.keepAliveTimeout = 120000
server.headersTimeout = 120000

const PORT = process.env.PORT || 3000
server.listen(PORT, () => console.log(`WhatsApp proxy listening on :${PORT}`))

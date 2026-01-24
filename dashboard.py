"""
Live Trade Feed - Real-time log viewer with JWT auth and PnL display.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import requests
from dotenv import load_dotenv
from fastapi import Cookie, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse

load_dotenv()

BASE_DIR = Path('/opt/trading_bot')
LOG_FILE = BASE_DIR / 'logs/trade.log'
TRADES_FILE = BASE_DIR / 'data/active_trades.json'
ALLOWED_PHONES_FILE = BASE_DIR / 'data/allowed_phones.json'

# Dhan API for PnL
DHAN_CLIENT_ID = os.getenv('DHAN_CLIENT_ID', '')
DHAN_ACCESS_TOKEN = os.getenv('DHAN_ACCESS_TOKEN', '')
DHAN_BASE_URL = 'https://api.dhan.co/v2'

# JWT Configuration
JWT_SECRET = os.getenv('JWT_SECRET', 'trading-dashboard-secret-key-change-me')
JWT_ALGORITHM = 'HS256'
JWT_EXPIRY_HOURS = 6  # Token expires after 6 hours

# PnL refresh interval (5 minutes)
PNL_REFRESH_SECONDS = 300

app = FastAPI(title='Live Trade Feed')

# PnL cache
pnl_cache = {'realized': 0.0, 'unrealized': 0.0, 'total': 0.0, 'updated_at': ''}
last_pnl_fetch = 0.0


def create_jwt_token(phone: str) -> str:
    """
    Create a JWT token for the given phone number.
    """
    payload = {
        'phone': phone,
        'exp': datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
        'iat': datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_jwt_token(token: str | None) -> dict | None:
    """
    Verify JWT token and return payload if valid.
    """
    if not token:
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None  # Token expired
    except jwt.InvalidTokenError:
        return None  # Invalid token


def fetch_pnl() -> dict:
    """
    Fetch realized + unrealized PnL from Dhan positions.
    """
    global last_pnl_fetch, pnl_cache

    now = time.time()
    if now - last_pnl_fetch < PNL_REFRESH_SECONDS:
        return pnl_cache

    try:
        headers = {
            'access-token': DHAN_ACCESS_TOKEN,
            'client-id': DHAN_CLIENT_ID,
            'Content-Type': 'application/json',
        }

        resp = requests.get(f'{DHAN_BASE_URL}/positions', headers=headers, timeout=10)
        # print(f"DEBUG: PnL Fetch Status: {resp.status_code}")

        if resp.status_code == 200:
            positions = resp.json()
            if isinstance(positions, dict):
                positions = positions.get('data', [])

            realized = 0.0
            unrealized = 0.0

            for p in positions:
                if isinstance(p, dict):
                    realized += float(p.get('realizedProfit', 0) or 0)
                    unrealized += float(p.get('unrealizedProfit', 0) or 0)

            pnl_cache = {
                'realized': realized,
                'unrealized': unrealized,
                'total': realized + unrealized,
                'updated_at': datetime.now().strftime('%H:%M:%S'),
            }
            last_pnl_fetch = now
        else:
            print(f'PnL Fetch Failed: {resp.status_code} {resp.text}')

    except Exception as e:
        print(f'PnL Fetch Error: {e}')
        pass

    return pnl_cache


def load_allowed_phones() -> set[str]:
    try:
        if ALLOWED_PHONES_FILE.exists():
            with open(ALLOWED_PHONES_FILE) as f:
                data = json.load(f)
                return set(str(p).strip() for p in data.get('phones', []))
    except Exception:
        pass
    return set()


def find_today_start() -> int:
    if not LOG_FILE.exists():
        return 0

    today = datetime.now().strftime('%Y-%m-%d')
    position = 0

    with open(LOG_FILE, 'rb') as f:
        while True:
            line = f.readline()
            if not line:
                break
            if today.encode() in line[:25]:
                return position
            position = f.tell()

    return position


@app.get('/', response_class=HTMLResponse)
async def index(token: str = Cookie(default=None)):
    if verify_jwt_token(token):
        return RedirectResponse('/feed', status_code=302)
    return LOGIN_PAGE


@app.post('/login')
async def login(request: Request):
    form = await request.form()
    phone = str(form.get('phone', '')).strip()

    # Normalize phone number
    phone = phone.replace(' ', '').replace('-', '')
    if phone.startswith('+91'):
        phone = phone[3:]
    if phone.startswith('91') and len(phone) == 12:
        phone = phone[2:]

    allowed = load_allowed_phones()

    if phone in allowed:
        jwt_token = create_jwt_token(phone)
        response = RedirectResponse('/feed', status_code=302)
        # Cookie expires when JWT expires (6 hours)
        response.set_cookie(
            'token', jwt_token, httponly=True, max_age=JWT_EXPIRY_HOURS * 3600, samesite='lax'
        )
        return response

    return HTMLResponse(
        LOGIN_PAGE.replace('<!-- ERROR -->', '<p class="error">Phone not authorized</p>')
    )


@app.get('/logout')
async def logout():
    response = RedirectResponse('/', status_code=302)
    response.delete_cookie('token')
    return response


@app.get('/feed', response_class=HTMLResponse)
async def feed(token: str = Cookie(default=None)):
    if not verify_jwt_token(token):
        return RedirectResponse('/', status_code=302)
    return FEED_PAGE


@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Get client's last known position from query string
    query_string = str(websocket.scope.get('query_string', b''), 'utf-8')
    client_pos = 0
    for param in query_string.split('&'):
        if param.startswith('pos='):
            try:
                client_pos = int(param.split('=')[1])
            except ValueError:
                pass

    # Use client position if valid, otherwise start from today
    today_start = find_today_start()
    if client_pos > today_start and LOG_FILE.exists() and client_pos <= LOG_FILE.stat().st_size:
        last_position = client_pos
    else:
        last_position = today_start

    pnl_counter = 0

    try:
        while True:
            # Logs
            if LOG_FILE.exists():
                current_size = LOG_FILE.stat().st_size
                if current_size > last_position:
                    with open(LOG_FILE, 'rb') as f:
                        f.seek(last_position)
                        new_data = f.read()
                        last_position = f.tell()

                    lines = new_data.decode('utf-8', errors='ignore').strip().split('\n')
                    for line in lines:
                        if line.strip():
                            await websocket.send_json({'type': 'log', 'data': line})

                    # Send updated position to client for caching
                    await websocket.send_json({'type': 'position', 'data': str(last_position)})

            # Trades
            try:
                if TRADES_FILE.exists():
                    with open(TRADES_FILE) as f:
                        trades = json.load(f)
                    await websocket.send_json({'type': 'trades', 'data': list(trades.values())})
            except Exception:
                pass

            # PnL every 10 iterations (5 sec), actual API throttled to 5 min
            pnl_counter += 1
            if pnl_counter >= 10:
                pnl_counter = 0
                pnl = fetch_pnl()
                await websocket.send_json({'type': 'pnl', 'data': pnl})

            await asyncio.sleep(0.5)

    except WebSocketDisconnect:
        pass


LOGIN_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login - Trade Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: #1a1d21;
            color: #f5f5f5;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            -webkit-font-smoothing: antialiased;
        }
        .login-box {
            background: #22262b;
            padding: 40px;
            border-radius: 16px;
            width: 90%;
            max-width: 380px;
            border: 1px solid #3a3f45;
        }
        h1 { 
            font-size: 24px; 
            font-weight: 600;
            margin-bottom: 8px; 
            text-align: center; 
        }
        .subtitle {
            color: #a0a5ad;
            font-size: 14px;
            text-align: center;
            margin-bottom: 32px;
        }
        input {
            width: 100%;
            padding: 16px;
            font-size: 18px;
            background: #1a1d21;
            border: 1px solid #3a3f45;
            border-radius: 10px;
            color: #f5f5f5;
            margin-bottom: 16px;
            text-align: center;
            letter-spacing: 2px;
        }
        input:focus { 
            outline: none; 
            border-color: #4a9eff;
            box-shadow: 0 0 0 3px rgba(74, 158, 255, 0.15);
        }
        input::placeholder {
            color: #6b7280;
            letter-spacing: normal;
        }
        button {
            width: 100%;
            padding: 16px;
            font-size: 16px;
            background: #4a9eff;
            color: #fff;
            border: none;
            border-radius: 10px;
            cursor: pointer;
            font-weight: 600;
            transition: background 0.2s;
        }
        button:hover { background: #3a8eef; }
        .error { 
            color: #ff453a; 
            text-align: center; 
            margin-bottom: 16px; 
            font-size: 14px;
            padding: 12px;
            background: rgba(255, 69, 58, 0.1);
            border-radius: 8px;
        }
    </style>
</head>
<body>
    <div class="login-box">
        <h1>Trade Dashboard</h1>
        <p class="subtitle">Enter your phone number to continue</p>
        <!-- ERROR -->
        <form method="POST" action="/login">
            <input type="tel" name="phone" placeholder="Phone Number" required autofocus>
            <button type="submit">Continue</button>
        </form>
    </div>
</body>
</html>
"""  # noqa: E501

FEED_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trade Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #1a1d21;
            --bg-secondary: #22262b;
            --bg-tertiary: #2a2f35;
            --text-primary: #f5f5f5;
            --text-secondary: #a0a5ad;
            --accent-blue: #4a9eff;
            --accent-green: #34c759;
            --accent-red: #ff453a;
            --accent-yellow: #ffd60a;
            --accent-purple: #bf5af2;
            --border: #3a3f45;
        }
        
        * { box-sizing: border-box; margin: 0; padding: 0; }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            -webkit-font-smoothing: antialiased;
        }
        
        .header {
            background: var(--bg-secondary);
            padding: 16px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
            position: sticky;
            top: 0;
            z-index: 100;
        }
        
        .title {
            display: flex;
            align-items: center;
            gap: 10px;
            font-size: 16px;
            font-weight: 600;
            color: var(--text-secondary);
        }
        
        .live-badge {
            display: flex;
            align-items: center;
            gap: 6px;
            background: rgba(52, 199, 89, 0.15);
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 500;
            color: var(--accent-green);
        }
        
        .live-dot {
            width: 8px;
            height: 8px;
            background: var(--accent-green);
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.6; transform: scale(0.9); }
        }
        
        .pnl-display {
            font-size: 28px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }
        
        .pnl-display.positive { color: var(--accent-green); }
        .pnl-display.negative { color: var(--accent-red); }
        .pnl-display.zero { color: var(--text-secondary); }
        
        .logout {
            font-size: 14px;
            color: var(--text-secondary);
            text-decoration: none;
            padding: 8px 16px;
            border-radius: 8px;
            transition: all 0.2s;
        }
        .logout:hover { 
            background: var(--bg-tertiary);
            color: var(--text-primary);
        }
        
        .trades-section {
            background: var(--bg-secondary);
            padding: 16px 20px;
            border-bottom: 1px solid var(--border);
        }
        
        .section-label {
            font-size: 12px;
            font-weight: 600;
            color: var(--text-secondary);
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 12px;
        }
        
        .trades-container {
            display: flex;
            gap: 10px;
            overflow-x: auto;
            padding-bottom: 4px;
        }
        
        .trade-chip {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 10px 16px;
            border-radius: 10px;
            font-size: 15px;
            font-weight: 600;
            white-space: nowrap;
        }
        
        .trade-chip.call {
            background: rgba(52, 199, 89, 0.12);
            color: var(--accent-green);
        }
        
        .trade-chip.put {
            background: rgba(255, 69, 58, 0.12);
            color: var(--accent-red);
        }
        
        .trade-chip::before {
            content: '';
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: currentColor;
        }
        
        .feed {
            flex: 1;
            padding: 16px 20px;
            overflow-y: auto;
            font-size: 15px;
            line-height: 1.8;
        }
        
        .line {
            padding: 6px 12px;
            margin-bottom: 4px;
            border-radius: 6px;
            animation: slideIn 0.3s ease;
            color: var(--text-secondary);
        }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateX(-10px); }
            to { opacity: 1; transform: translateX(0); }
        }
        
        .line.imbalance { 
            background: rgba(255, 214, 10, 0.08);
            color: var(--accent-yellow);
        }
        .line.error { 
            background: rgba(255, 69, 58, 0.08);
            color: var(--accent-red);
        }
        .line.success { 
            background: rgba(52, 199, 89, 0.08);
            color: var(--accent-green);
        }
        .line.order { 
            background: rgba(74, 158, 255, 0.08);
            color: var(--accent-blue);
        }
        .line.exit { 
            background: rgba(191, 90, 242, 0.08);
            color: var(--accent-purple);
        }
        
        .no-trades { 
            color: var(--text-secondary);
            font-style: normal;
            font-size: 14px;
        }
        
        /* Scrollbar styling */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: var(--bg-primary); }
        ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
        ::-webkit-scrollbar-thumb:hover { background: #4a4f55; }
        
        /* Auto-scroll button */
        .auto-scroll-btn {
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: var(--bg-tertiary);
            color: var(--text-secondary);
            border: 1px solid var(--border);
            padding: 10px 16px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            z-index: 100;
            transition: all 0.2s;
        }
        .auto-scroll-btn:hover {
            background: var(--bg-secondary);
            color: var(--text-primary);
        }
        .auto-scroll-btn.active {
            background: var(--accent-green);
            color: #fff;
            border-color: var(--accent-green);
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="live-badge">
            <div class="live-dot"></div>
            <span>LIVE</span>
        </div>
        <div class="pnl-display zero" id="pnl">₹0</div>
        <a href="/logout" class="logout">Logout</a>
    </div>
    
    <div class="trades-section">
        <div class="section-label">Active Trades</div>
        <div class="trades-container" id="trades">
            <span class="no-trades">No active trades</span>
        </div>
    </div>
    
    <div class="feed" id="feed"></div>
    
    <button class="auto-scroll-btn" id="autoScrollBtn" onclick="toggleAutoScroll()">
        ⬇ Auto
    </button>

    <script>
        const feed = document.getElementById('feed');
        const tradesBar = document.getElementById('trades');
        const pnlEl = document.getElementById('pnl');
        const autoScrollBtn = document.getElementById('autoScrollBtn');
        
        let ws;
        let autoScroll = true;
        let isProgrammaticScroll = false;
        let scrollFrameId = null;
        let scrollUnlockTimer = null;
        
        // Optimized scroll handler
        feed.addEventListener('scroll', () => {
            if (isProgrammaticScroll) return;

            const atBottom = feed.scrollHeight - feed.scrollTop - feed.clientHeight < 50;
            
            if (!atBottom && autoScroll) {
                autoScroll = false; // User scrolled up
                updateScrollBtn();
            } else if (atBottom && !autoScroll) {
                autoScroll = true; // User scrolled back to bottom
                updateScrollBtn();
            }
        });
        
        function toggleAutoScroll() {
            autoScroll = !autoScroll;
            updateScrollBtn();
            if (autoScroll) {
                scrollToBottom();
            }
        }
        
        function scrollToBottom() {
            // Lock immediately to prevent race conditions during heavy load
            isProgrammaticScroll = true;
            
            if (scrollFrameId) cancelAnimationFrame(scrollFrameId);
            
            scrollFrameId = requestAnimationFrame(() => {
                feed.scrollTop = feed.scrollHeight;
                scrollFrameId = null;
                
                // Clear existing unlock timer
                if (scrollUnlockTimer) clearTimeout(scrollUnlockTimer);
                
                // Reset flag after browser processes scroll + buffer
                scrollUnlockTimer = setTimeout(() => { 
                    isProgrammaticScroll = false; 
                }, 100);
            });
        }
        
        function updateScrollBtn() {
            autoScrollBtn.classList.toggle('active', autoScroll);
            autoScrollBtn.textContent = autoScroll ? '⬇ Auto' : '⬇ Paused';
        }
        
        function connect() {
            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            // Send last known position to avoid reloading all logs
            const lastPos = sessionStorage.getItem('logPosition') || '0';
            ws = new WebSocket(`${protocol}//${location.host}/ws?pos=${lastPos}`);
            
            ws.onclose = () => setTimeout(connect, 2000);
            
            ws.onmessage = (event) => {
                const msg = JSON.parse(event.data);
                if (msg.type === 'log') addLogLine(msg.data);
                else if (msg.type === 'trades') updateTrades(msg.data);
                else if (msg.type === 'pnl') updatePnL(msg.data);
                else if (msg.type === 'position') sessionStorage.setItem('logPosition', msg.data);
            };
        }
        
        function addLogLine(text) {
            const line = document.createElement('div');
            line.className = 'line';
            
            if (text.includes('IMB')) line.className += ' imbalance';
            else if (text.includes('ERROR') || text.includes('❌')) line.className += ' error';
            else if (text.includes('SUCCESS') || text.includes('✅')) line.className += ' success';
            else if (text.includes('EXECUTING')) line.className += ' order';
            else if (text.includes('Exit') || text.includes('SQUARED')) line.className += ' exit';
            
            line.textContent = text;
            feed.appendChild(line);
            
            // Keep buffer size reasonable
            while (feed.children.length > 500) feed.removeChild(feed.firstChild);
            
            if (autoScroll) {
                scrollToBottom();
            }
        }
        
        function updateTrades(trades) {
            if (!trades.length) {
                tradesBar.innerHTML = '<span class="no-trades">No active trades</span>';
                return;
            }
            tradesBar.innerHTML = trades.map(t => {
                const type = t.is_call ? 'call' : 'put';
                return `<span class="trade-chip ${type}">${t.symbol || 'Unknown'}</span>`;
            }).join('');
        }
        
        function updatePnL(data) {
            const total = data.total || 0;
            pnlEl.textContent = '₹' + total.toLocaleString('en-IN', {maximumFractionDigits: 0});
            pnlEl.className = 'pnl-display ' + (total > 0 ? 'positive' : total < 0 ? 'negative' : 'zero');
        }
        
        // Clear position on new day
        const today = new Date().toDateString();
        if (sessionStorage.getItem('logDate') !== today) {
            sessionStorage.removeItem('logPosition');
            sessionStorage.setItem('logDate', today);
        }
        
        updateScrollBtn();
        connect();
    </script>
</body>
</html>
"""  # noqa: E501

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host='127.0.0.1', port=8090)

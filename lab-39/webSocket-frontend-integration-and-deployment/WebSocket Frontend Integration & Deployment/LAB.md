# Lab: WebSocket Frontend Integration & Deployment

## Introduction

Real-time systems let users see task results and progress as soon as they are produced. In this lab, Celery workers publish task events through Redis Pub/Sub channels. A Python Socket.IO server forwards these events to connected clients for live updates. Finally, the complete system is deployed behind Nginx with WebSocket support on Poridhi Cloud.

## Architecture

![Architecture Diagram](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39architecturediagram.png)

### Why pub/sub, not Celery events

* Celery events are an internal monitoring stream; they are designed for Flower-style tooling, not for end-user UIs.
* Per-task Redis channels give us a clean, stateless routing key — exactly one publisher, exactly the interested subscribers.

## Objectives 

By the end of this lab you will:

1. Publish Celery task lifecycle events to a Redis channel `task:<id>`.
2. Bridge those events to the right Socket.IO room via `python-socketio`.
3. Serve a small HTML/JS frontend that submits tasks and renders the live stream.
4. Deploy the full stack behind nginx on Cloud with WebSocket upgrade support.



## Step 1 — Prerequisites

1. Tools: Node.js + npm, Python3 + pip, Redis server, NGINX — all must be installed on the  VM (needs sudo access)
2. Skills: Basic Linux terminal commands, general familiarity with JavaScript/Node.js, ability to read basic Python syntax
3. Concepts: A basic understanding of how Redis Pub/Sub and WebSockets work (not mandatory — can be picked up while doing the lab)
4. Platform: Access to the Load Balancer/Cloud Tray feature in the Poridhi dashboard (to publicly expose a port), plus internet access from the VM (for npm/pip package downloads)

## Step 2 — Install Redis and NGINX


Skip Docker entirely — Redis only needs to run as a local system service for this lab, so it was installed directly instead of via a container.

```bash
sudo apt update
sudo apt install -y redis-server nginx
```
![Lab 39 - 1](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_1.png)

Start and enable Redis:

```bash
sudo systemctl start redis-server
sudo systemctl enable redis-server
redis-cli ping
```

![Lab 39 - 2](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_2.png)

## Step 3 — Create the project structure

```bash
cd ~/code
mkdir -p lab39-websocket/{server,frontend,producer,nginx}
cd lab39-websocket
touch server/package.json server/index.js
touch frontend/index.html
touch producer/publish_task.py
touch nginx/websocket.conf

```
```bash
find . -not -path '*/node_modules/*'
```

![Lab 39 - 3](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_3.png)


## Step 4 — `server/package.json` and dependency install

```bash
cat > server/package.json << 'EOF'
{
  "name": "lab39-ws-server",
  "version": "1.0.0",
  "main": "index.js",
  "scripts": {
    "start": "node index.js"
  },
  "dependencies": {
    "express": "^4.19.2",
    "socket.io": "^4.7.5",
    "ioredis": "^5.4.1"
  }
}
EOF

cd server
npm install
```
![Lab 39 - 4](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_4.png)

npm reported 3 moderate-severity advisories and an npm-version notice — both informational only, no action needed for this lab.

```bash
ls node_modules | grep -E 'express|socket.io|ioredis'
```



---


## Step 5 — `server/index.js` (WebSocket server: Redis subscriber + Socket.IO rooms)

```bash
cat > index.js << 'EOF'
const express = require("express");
const http = require("http");
const path = require("path");
const { Server } = require("socket.io");
const Redis = require("ioredis");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*" },
});

const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = process.env.REDIS_PORT || 6379;

const redisPub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
const redisSub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });

app.use(express.static(path.join(__dirname, "../frontend")));
app.use(express.json());

app.post("/api/start-task", async (req, res) => {
  const taskId = "task-" + Math.random().toString(36).slice(2, 8);
  res.json({ task_id: taskId });
});

redisSub.psubscribe("task:*", (err, count) => {
  if (err) {
    console.error("Failed to subscribe:", err);
    return;
  }
  console.log(`Subscribed to Redis pattern "task:*" (${count} pattern(s))`);
});

redisSub.on("pmessage", (pattern, channel, message) => {
  const taskId = channel.split(":")[1];
  console.log(`[redis] ${channel} -> room "${taskId}":`, message);

  let payload;
  try {
    payload = JSON.parse(message);
  } catch (e) {
    payload = { raw: message };
  }

  io.to(taskId).emit("task_update", payload);
});

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on("subscribe_task", (taskId) => {
    socket.join(taskId);
    console.log(`Socket ${socket.id} joined room "${taskId}"`);
    socket.emit("subscribed", { task_id: taskId });
  });

  socket.on("unsubscribe_task", (taskId) => {
    socket.leave(taskId);
  });

  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`WebSocket server listening on http://0.0.0.0:${PORT}`);
});
EOF
```

### Issue hit: `EADDRINUSE :::3000`

```bash
node index.js
```
![Lab 39 - 5](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_5.png)


Error: listen EADDRINUSE: address already in use :::3000
```

**Diagnosis:**

```bash
sudo ss -lntp | grep 3000
ps aux | grep node
```

Port 3000 was occupied by `poridhi-terminal-standalone.js` — a **Poridhi platform system process**, not our app, and it must not be killed.


**Resolution:** Run the app on a different port instead of fighting the platform for port 3000.

```bash
PORT=3001 node index.js
```

**Expected output:**
```
Subscribed to Redis pattern "task:*" (1 pattern(s))
WebSocket server listening on http://0.0.0.0:3001
```


> Leave this terminal running for the rest of the lab.

---

## Step 6 — `frontend/index.html` (Socket.IO client)

First version (absolute paths):

```bash
cd ~/code/lab39-websocket/frontend
cat > index.html << 'EOF'
<!-- ... script src="/socket.io/socket.io.js", fetch("/api/start-task") ... -->
EOF
```

Opened the app via the VS Code proxy URL:
```
https://<vm-id>.vscode.poridhi.io/proxy/3001/
```

Clicking **Start New Task** produced nothing, and the browser console showed:

```
socket.io.js:1  Failed to load resource: the server responded with a status of 404 ()
3001/:25 Uncaught ReferenceError: io is not defined
```


### Diagnosis

Confirmed the backend itself was fine:

```bash
curl -X POST http://127.0.0.1:3001/api/start-task
# {"task_id":"task-c3g9fx"}
```
![Lab 39 - 7](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_7.png)

curl -I http://127.0.0.1:3001/socket.io/socket.io.js
# HTTP/1.1 200 OK ...
```
![Lab 39 - 8](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_8.png)



### Fix — use a dynamic base path derived from the current URL

```bash
cd ~/code/lab39-websocket/frontend
cat > index.html << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<title>Lab 39 - Live Task Progress</title>
<script src="socket.io/socket.io.js"></script>
<style>
  body { font-family: system-ui, sans-serif; max-width: 560px; margin: 40px auto; }
  #bar-wrap { background: #eee; border-radius: 8px; overflow: hidden; height: 24px; margin: 12px 0; }
  #bar { background: #4caf50; height: 100%; width: 0%; transition: width .3s ease; }
  #log { background: #111; color: #0f0; font-family: monospace; padding: 12px;
         height: 180px; overflow-y: auto; border-radius: 6px; font-size: 13px; }
  button { padding: 8px 16px; cursor: pointer; }
</style>
</head>
<body>
  <h2>Lab 39: Live Task Progress (Redis → WebSocket → Browser)</h2>
  <button id="startBtn">Start New Task</button>
  <p>Task ID: <code id="taskId">-</code></p>
  <div id="bar-wrap"><div id="bar"></div></div>
  <p>Status: <span id="status">idle</span></p>
  <div id="log"></div>

  <script>
    // Base path = current page's folder — works whether accessed directly
    // or through Poridhi's /proxy/3001/ prefix
    const basePath = window.location.pathname.replace(/\/[^/]*$/, "/");

    const socket = io({ path: basePath + "socket.io/" });

    const startBtn = document.getElementById("startBtn");
    const taskIdEl = document.getElementById("taskId");
    const bar = document.getElementById("bar");
    const statusEl = document.getElementById("status");
    const logEl = document.getElementById("log");

    function log(msg) {
      const line = document.createElement("div");
      line.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
      logEl.appendChild(line);
      logEl.scrollTop = logEl.scrollHeight;
    }

    socket.on("connect", () => log("Connected to server: " + socket.id));
    socket.on("connect_error", (err) => log("Connect error: " + err.message));
    socket.on("disconnect", () => log("Disconnected from server"));

    socket.on("subscribed", (data) => {
      log(`Subscribed to updates for ${data.task_id}`);
    });

    socket.on("task_update", (data) => {
      log(`Update: progress=${data.progress}% status=${data.status}`);
      bar.style.width = data.progress + "%";
      statusEl.textContent = data.status;
    });

    startBtn.addEventListener("click", async () => {
      const res = await fetch(basePath + "api/start-task", { method: "POST" });
      const data = await res.json();
      taskIdEl.textContent = data.task_id;
      bar.style.width = "0%";
      statusEl.textContent = "waiting for producer...";
      log(`Got task_id ${data.task_id}. Run: python3 producer/publish_task.py ${data.task_id}`);
      socket.emit("subscribe_task", data.task_id);
    });
  </script>
</body>
</html>
EOF
```

Hard-refreshed the browser (`Ctrl+Shift+R`) and clicked **Start New Task** again — a `task_id` was generated successfully with no console errors.


## Step 7 — `producer/publish_task.py` (simulated worker)

```bash
cd ~/code/lab39-websocket/producer
cat > publish_task.py << 'EOF'
import redis
import json
import time
import sys
import uuid

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)


def run_task(task_id: str):
    channel = f"task:{task_id}"
    stages = [
        (0, "queued"),
        (25, "processing"),
        (50, "processing"),
        (75, "almost done"),
        (100, "completed"),
    ]

    for progress, status in stages:
        payload = {
            "task_id": task_id,
            "progress": progress,
            "status": status,
            "timestamp": time.time(),
        }
        r.publish(channel, json.dumps(payload))
        print(f"Published -> {channel}: {payload}")
        time.sleep(2)


if __name__ == "__main__":
    task_id = sys.argv[1] if len(sys.argv) > 1 else f"task-{uuid.uuid4().hex[:6]}"
    print(f"Simulating task: {task_id}")
    run_task(task_id)
EOF

pip install redis --break-system-packages
```

![Lab 39 - 6](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_6.png)


## Step 8 — First end-to-end test (via port 3001 proxy)

1. Opened `https://<vm-id>.vscode.poridhi.io/proxy/3001/`, clicked **Start New Task**, copied the generated `task_id`.
2. Ran the producer with that ID:
   ```bash
   cd ~/code/lab39-websocket/producer
   python3 publish_task.py task-xxxxxx
   ```

**Confirmed in three places simultaneously:**
- Producer terminal: 5 `Published -> task:task-xxxxxx: {...}` lines, one every 2 seconds
- Server terminal: `[redis] task:task-xxxxxx -> room "task-xxxxxx": ...` logs
- Browser: progress bar filling 0% → 25% → 50% → 75% → 100%, status text updating, log panel showing each update

`[SCREENSHOT: producer terminal output]`
`[SCREENSHOT: server terminal showing the [redis] ... -> room logs]`
`[SCREENSHOT: browser with progress bar at 100% / "completed"]`

✅ **Core requirement verified:** Redis subscription → task-ID-based forwarding → live frontend update, all working end-to-end.

---


## Step 9 — NGINX reverse proxy with WebSocket support

### 9.1 Config file

```bash
sudo tee /etc/nginx/sites-available/lab39 > /dev/null << 'EOF'
map $http_upgrade $connection_upgrade {
    default upgrade;
    ''      close;
}

server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:3001;

        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }
}
EOF
```

### 9.2 Enable the site and disable the default

```bash
sudo ln -sf /etc/nginx/sites-available/lab39 /etc/nginx/sites-enabled/lab39
sudo rm -f /etc/nginx/sites-enabled/default
```

### 9.3 Test config, start, reload

```bash
sudo nginx -t
sudo systemctl start nginx
sudo systemctl reload nginx
sudo systemctl status nginx --no-pager
```

**Result:** `syntax is ok` / `test is successful`; service log showed `Started nginx.service` and successive `Reloaded nginx.service` entries with no errors.

![Lab 39 - 10](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_10.png)

### 9.4 Verify NGINX is forwarding to the Node app

```bash
curl -I http://127.0.0.1:80/

```

```
sudo ss -lntp | grep :80
```
![Lab 39 - 12](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_12.png)


![Lab 39 - 11](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_11.png)

**Result:** the `curl` response included `X-Powered-By: Express` — proof that NGINX (port 80) is proxying to the Express/Socket.IO app (port 3001). `ss` confirmed nginx worker processes listening on `0.0.0.0:80`.


## Step 10 — Exposing port 80 publicly via Poridhi Load Balancer

The `/proxy/<port>/` VS Code URL pattern used for port 3001 does **not** apply the same way for arbitrary ports at the platform level — Poridhi instead provides a **Load Balancer** feature tied to the VM's `wt0` (WARP tunnel) interface IP.

### 10.1 Get the `wt0` IP

```bash
ip addr show wt0
```
![Lab 39 - 13](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_13.png)

**Result:** `inet 100.80.102.179/16 ...`



> ⚠️ Must use the `wt0` interface IP, not `eth0`.

### 10.2 Create the Load Balancer

In the Poridhi dashboard:
1. Open **Load Balancer** (Cloud Tray).
2. Create a new Load Balancer with:
   - **IP:** `100.80.102.179`
   - **Port:** `80`


![Load Balancer](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39loadbalamcerimage.png)

![After Load Balancer](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39afterloadbalancer.png)

## Step 11 — Final end-to-end verification (through NGINX + Load Balancer)

1. Opened the Poridhi Load Balancer public URL in the browser.
2. Clicked **Start New Task** — a `task_id` was returned successfully (confirming NGINX → Node → Express routing works over the public URL).
3. Ran the producer with that task ID:
   ```bash
   cd ~/code/lab39-websocket/producer
   python3 publish_task.py <task_id>
   ```
   
![Lab 39 - 14](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39_14.png)


4. Confirmed the progress bar updated live 0% → 100% on the public Load-Balancer URL, and checked DevTools → Network → WS to confirm a `101 Switching Protocols` WebSocket upgrade succeeded through NGINX.

![Last Image 2](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39lastimage2.png)


![Task ID Output](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39taskidoutput.png)


---

## Final Result

| Component | Status |
|---|---|
| Redis running as a system service | ✅ |
| Node.js server subscribed to `task:*` via `PSUBSCRIBE` | ✅ |
| Task-ID-based routing via Socket.IO rooms | ✅ |
| Frontend (Socket.IO client) live progress UI | ✅ |
| Python producer simulating a worker | ✅ |
| NGINX reverse proxy with WebSocket `Upgrade`/`Connection` headers | ✅ |
| Public deployment via Poridhi Load Balancer (`wt0` IP : 80) | ✅ |
| End-to-end verified: Redis → Node → NGINX → public browser | ✅ |


![Progress Redis WebSocket](https://raw.githubusercontent.com/poridhioss/python-lab-asset/d192b52a0d4b3f4dfa1f4ddf5af291b2248c8337/lab-39progressrediswebsocket.png)


## Conclusion

Celery worker publishes lifecycle events to a per-task Redis pub/sub channel
A python-socketio server subscribes to those channels and forwards messages into Socket.IO rooms keyed by task ID — with refcounted background tasks so we never leak Redis subscribers.
single static HTML page with the Socket.IO client renders the live stream and gives users instant feedback instead of polling.
The whole stack is deployed behind nginx with WebSocket upgrade headers, ready to run on  Cloud (or AWS EC2 / ALB) in production.
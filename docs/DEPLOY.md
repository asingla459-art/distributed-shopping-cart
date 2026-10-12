# Deployment guide

The whole stack (nginx LB → 3 app instances → Redis primary + replica, plus the order
worker) is defined in `docker-compose.yml` and runs on any Docker host. The steps
below use **AWS EC2** (free-tier eligible). Azure and GCP equivalents are at the end.

## A. Deploy on AWS EC2 (about 15 minutes)

1. **Launch the VM**
   AWS console → EC2 → *Launch instance*
   * Name `dscart`, AMI **Ubuntu Server 24.04 LTS**, type **t3.small** (t2.micro/t3.micro also works for the demo)
   * Key pair: create one and download the `.pem`
   * Network settings → *Edit* → security group rules:
     `SSH 22` from *My IP*, `HTTP 80` from *Anywhere (0.0.0.0/0)`
   * Storage 16 GB → *Launch*

2. **Install Docker** (SSH in: `ssh -i dscart.pem ubuntu@<public-ip>`)
   ```bash
   curl -fsSL https://get.docker.com | sudo sh
   sudo usermod -aG docker $USER && newgrp docker
   ```

3. **Get the code and configure secrets**
   ```bash
   git clone https://github.com/<your-user>/distributed-shopping-cart.git dscart && cd dscart
   cp .env.example .env
   sed -i "s/^JWT_SECRET=.*/JWT_SECRET=$(openssl rand -hex 32)/; s/^REDIS_PASSWORD=.*/REDIS_PASSWORD=$(openssl rand -hex 16)/" .env
   ```

4. **Start the stack and seed data**
   ```bash
   docker compose up -d --build
   docker compose ps                      # lb, app1-3, worker, redis, redis-replica all "running"/"healthy"
   docker compose exec app1 python -m app.seed
   ```

5. **Verify from your laptop**
   ```bash
   curl http://<public-ip>/health         # repeat: X-Instance rotates app-1/2/3
   BASE=http://<public-ip> experiments/demo.sh api
   ```

6. **Run the demo / experiments against the cloud**
   ```bash
   # on the VM (Redis is private, so redis-cli runs inside the container):
   source .env
   BASE=http://localhost RCLI="docker compose exec -T redis redis-cli -a $REDIS_PASSWORD --no-auth-warning" \
     experiments/demo.sh
   docker compose logs -f app1 app2 app3   # JSON logs
   ```
   Load test from your laptop (needs Python 3.11+, no packages):
   `TARGET_HOST=<public-ip> TARGET_PORT=80 python3 -m loadtest.loadgen --users 25 --duration 20 --label medium`
   (the cache-hit-ratio field needs direct instance ports, so it is only filled in for local runs).

7. **Screenshots for the report**: EC2 console showing the running instance, `docker compose ps`,
   the demo output, and `curl http://<public-ip>/api/v1/products` in a browser.

8. **Stop paying**: `docker compose down` and *Terminate* the instance when you are done.

## B. Using managed cloud services instead (production variant)

| Component | Local / VM | AWS | Azure | GCP |
|---|---|---|---|---|
| Load balancer | nginx container | Application Load Balancer | Application Gateway | Cloud Load Balancing |
| App instances | 3 containers | ECS Fargate service (3 tasks) / Auto Scaling Group | Container Apps (3 replicas) | Cloud Run (min 3) |
| Redis | redis + replica containers | ElastiCache for Redis (Multi-AZ, auto failover) | Azure Cache for Redis | Memorystore |
| Logs / metrics | docker logs, `/metrics` | CloudWatch Logs + Managed Prometheus | Azure Monitor | Cloud Logging / Monitoring |

Only environment variables change: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`, `JWT_SECRET`.
The service is stateless, so any of these platforms can add or remove instances freely.

## C. Azure / GCP VM quick path
* **Azure**: Create a VM (Ubuntu 24.04, B2s), open inbound port 80 in its NSG, then steps 2–5 above.
* **GCP**: Compute Engine → e2-small, Ubuntu 24.04, tick *Allow HTTP traffic*, then steps 2–5.

## D. Run locally without Docker (what the experiments in the report used)
```bash
# needs: python3 (3.11+), redis-server, pip install -r requirements.txt
deploy/local/cluster.sh start consistent 3     # LB on :8080, instances on :8001-8003
experiments/demo.sh                            # full demonstration
deploy/local/cluster.sh stop
```

## Operations cheat-sheet

| Task | Command |
|---|---|
| Crash an instance (auto-restarts) | `POST /api/v1/admin/chaos {"instance":"app-2","mode":"crash_now"}` |
| Scale out | add `app4` to `docker-compose.yml` and to the nginx `upstream`, `docker compose up -d` |
| Promote replica if primary is lost | `docker compose exec redis-replica redis-cli -a $REDIS_PASSWORD REPLICAOF NO ONE`, then point `REDIS_HOST` to `redis-replica` |
| Switch to the baseline for comparison | `CONSISTENCY_MODE=naive docker compose up -d` |
| Watch conflicts/replays | `curl http://<ip>/metrics \| grep -E 'conflicts\|replays'` |

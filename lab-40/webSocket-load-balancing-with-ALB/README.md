## WebSocket Load Balancing with AWS Application Load Balancer

## 1.Introduction

This lab deploys a FastAPI WebSocket application behind an AWS Application Load Balancer, provisioned with Terraform across EC2 instances in an Auto Scaling Group. It walks through the full WebSocket flow — from the HTTP 101 upgrade to the connection staying pinned on its backend — while separately verifying ALB cookie stickiness for normal HTTP requests. The lab also shows how multiple EC2 instances handle concurrent WebSocket clients, with optional HTTPS/wss:// support.

 ## Architecture
lab-40architecturediagram

## Objectives

After completing this lab, you should be able to:

Explain the WebSocket upgrade mechanism and deploy a FastAPI WebSocket app with Docker on AWS.
Provision an ALB, target group, and EC2 infrastructure using Terraform, with health checks.
Test WebSocket connectivity (`ws://`/`wss://`) and run concurrent load tests across backends.
Distinguish HTTP cookie stickiness from WebSocket connection persistence.

## 2.Prerequisites
AWS account with appropriate permissions
Terraform >= 1.0
Docker
Python 3.9+
AWS CLI configured




### Local 

```bash
git --version
python3 --version
docker --version
docker compose version
terraform version
aws --version
```

Required: Git, Python 3.9+, Docker + Compose plugin, Terraform ≥ 1.5, AWS CLI v2.

### AWS Setup

imageawscredimage

```bash
aws configure
```
imageawsconfigeimage

```bash
aws sts get-caller-identity
```
lab-40image_4

The configured AWS identity needs permission to create a VPC, EC2/Auto Scaling resources, an ALB, a target group, and associated security groups.

> **Recommended region:** `ap-southeast-1` (Singapore) — this is also the Terraform default.



## 5. Project Structure

```text
lab-40/
├── README.md
├── app/
│   ├── main.py
│   ├── requirements.txt
│   ├── Dockerfile
│   └── docker-compose.yml
├── infrastructure/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       ├── user_data.sh
│       └── terraform.tfvars
├
└── tests/
    ├── requirements.txt
    ├── test_websocket.py
    ├── test_stickiness.py
    └── load_test.py
```


## 6. Application Overview

Built with **FastAPI**.

**HTTP endpoints**

```text
GET /
GET /health
GET /instance
```

**WebSocket endpoint**

```text
/ws
```
Each WebSocket response includes the instance name, connection ID, echoed message, and a timestamp.

---

```
curl http://127.0.0.1:8000/health

```
image7_8

## 7. Part 1 — Get the Project

```bash
cd lab-40
```

Otherwise, clone it:

```bash
git clone <YOUR-REPOSITORY-URL> lab-40
cd lab-40
```


## 8. Part 2 — Local Docker Test (recommended before AWS)

```bash
cd app
docker compose up --build -d
```
image5

```bash
docker compose ps
```
image6

```bash
curl http://127.0.0.1:8001/health
```
image7 sathe running image

healthimage
```json
{"status":"healthy"}
```

```bash
cd ../tests
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
image8

```bash
python test_websocket.py ws://127.0.0.1:8000/ws
```
image9 khali

```text
PASS: WebSocket connection and echo test succeeded.
```


## 9. Part 3 — Deploy AWS Infrastructure with Terraform

```bash
terraform init
```
image10
```bash
terraform validate
```
image11

```bash
terraform providers
aws configure get region
aws ec2 describe-vpcs \
  --region ap-southeast-1 \
  --query 'Vpcs[*].[VpcId,IsDefault,CidrBlock]' \
  --output table
```

```bash
terraform plan
```
image12

```bash
terraform apply
```
image13

Confirm with `yes` when prompted. **The default deployment creates:**

- 1 VPC
- 2 public subnets across 2 Availability Zones
- Internet Gateway
- ALB + ALB security group
- EC2 security group
- Target group
- Auto Scaling Group with 2 EC2 instances
- HTTP listener on port 80
- Target-group cookie stickiness
- `/health` ALB health check
- 300-second ALB idle timeout
- FastAPI WebSocket service on each EC2 instance

```text
No changes. Your infrastructure matches the configuration.
```

![Terraform apply output showing no changes / successful convergence](docs/images/terraform-plan.svg)

---

## 10. Part 4 — Verify AWS Deployment

```bash
terraform output
```
image14

```bash
terraform output -raw alb_dns_name
```

```bash
curl http://$(terraform output -raw alb_dns_name)/health
```

```json
{
  "status": "healthy",
  "instance": "app1"
}
```
image15



## 11. Part 5 — Test WebSocket Through the ALB

```bash
cd ../../tests
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

```bash
python3 test/test_websocket.py
```
image16

```text
Connected successfully!
Instance: app2
Connection: 0835f1e0

Message: hello-0 | Instance: app2
Message: hello-1 | Instance: app2
Message: hello-2 | Instance: app2
Message: hello-3 | Instance: app2
Message: hello-4 | Instance: app2

PASS: WebSocket connection stayed on: app2
```


## 12. Part 6 — Verify Session Stickiness Correctly

```bash

python test_stickiness.py 

```
image17
The test reuses a single `requests.Session()` so the ALB-generated cookie is retained across requests.

```text
Request 1: Instance = app1
Request 2: Instance = app1
Request 3: Instance = app1
Request 4: Instance = app1
Request 5: Instance = app1
Request 6: Instance = app1
Request 7: Instance = app1
Request 8: Instance = app1
Request 9: Instance = app1
Request 10: Instance = app1

PASS: HTTP session remained on app1
```

The session retained the ALB-generated `AWSALB` and `AWSALBCORS` cookies.


### What this test proves

1. The target group has cookie stickiness enabled.
2. A client retaining the ALB cookie stays on the same backend for HTTP requests.
3. The WebSocket handshake upgrades successfully.
4. After `101 Switching Protocols`, the WebSocket TCP connection stays on the selected backend.

### What this test does **not** prove

It does **not** prove that the ALB cookie is consulted for every WebSocket message — AWS states that WebSocket connections are inherently sticky after upgrade, and cookie-based stickiness is not used past that point.

---

## 13. Part 7 — Load Test

**Configuration used:**

```python
CLIENTS = 50
MESSAGES_PER_CLIENT = 20
```

```bash
WS_URL=$(cd ../infrastructure/terraform && terraform output -raw websocket_url_http)
python load_test.py "$WS_URL" 20 30      # 20 clients, 30 seconds
```
image18

```bash
python load_test.py "$WS_URL" 50 30      # larger run — 50 clients, 20 msgs each
```
image19

```text
Successful clients: 50
Failed clients: 0
Total messages: 1000
Total time: 0.39 seconds
Throughput: 2575.25 msg/sec

Backend distribution:
  app1: 25 clients
  app2: 25 clients

PASS: All WebSocket clients completed successfully!
```

Exact distribution isn't guaranteed — the goal is to confirm many simultaneous WebSocket connections can be established and maintained through the ALB.

---

## 14. Part 8 — AWS Console Verification

**Region:** `Asia Pacific (Singapore)`

### 14.1 EC2

`EC2 → Instances` — expect ~2 running instances named similarly to `lab40-websocket-ec2`.

### 14.2 Target Group

`EC2 → Load Balancing → Target Groups → lab40-websocket-tg → Targets → Health status`

```text
i-0ad7aab16d6759c24    8000    healthy
i-0863cf71a306ef5e3    8000    healthy
```

![Target group console view showing both EC2 targets healthy](docs/images/target-health.svg)

If either target is unhealthy, check `http://INSTANCE_PRIVATE_IP:8000/health` from an appropriate network path and inspect the systemd service (see Troubleshooting, Section 16).

### 14.3 ALB

`EC2 → Load Balancers → lab40-websocket-alb` — verify the `HTTP :80` listener (and `HTTPS :443` if enabled).


   ```hcl
   aws_region          = "ap-southeast-1"
   enable_https        = true
   acm_certificate_arn = "YOUR_ACM_CERTIFICATE_ARN"
   ```

   ```bash
   terraform plan
   terraform apply
   ```

5. **Test WSS** once DNS resolves to the ALB:

   ```bash
   python test_websocket.py wss://ws.example.com/ws
   ```

   ```text
   PASS: WebSocket connection and echo test succeeded.
   ```



## Useful AWS CLI Commands

```bash
# List ALBs
aws elbv2 describe-load-balancers --region ap-southeast-1
```
image20

```bash
# List target groups
aws elbv2 describe-target-groups --region ap-southeast-1
```


```bash
# Check target health
aws elbv2 describe-target-health \
  --target-group-arn "$(terraform output -raw target_group_arn)" \
  --region ap-southeast-1 \
  --query 'TargetHealthDescriptions[].[Target.Id,Target.Port,TargetHealth.State]' \
  --output table
```
image20

```bash
# List EC2 instances for this project
aws ec2 describe-instances \
  --region ap-southeast-1 \
  --filters "Name=tag:Project,Values=lab40-websocket"
```

```bash
# Stickiness attributes
aws elbv2 describe-target-group-attributes \
  --target-group-arn "$(terraform output -raw target_group_arn)" \
  --region ap-southeast-1 \
  --query 'Attributes[?starts_with(Key, `stickiness`) || Key==`load_balancing.algorithm.type`].[Key,Value]' \
  --output table
```
imagge21

```text
stickiness.type                        lb_cookie
stickiness.lb_cookie.duration_seconds  86400
stickiness.enabled                     true
load_balancing.algorithm.type          round_robin
```

During the load test, confirm the healthy target count stays stable while active connections rise.


## Conclusion

Demonstrates a production-relevant WebSocket deployment pattern using FastAPI, Docker, EC2, an Auto Scaling Group, an Application Load Balancer, target health checks, and optional TLS termination.



---
name: poridhi-lab-40-websocket-alb
description: Complete Poridhi lab for WebSocket Load Balancing with AWS Application Load Balancer (ALB). Covers ALB setup with WebSocket listeners, target group stickiness, FastAPI WebSocket server deployment, and comprehensive testing.
allowed-tools:
  - Bash(terraform:*)
  - Bash(docker:*)
  - Bash(aws:*)
  - Bash(python:*)
  - Bash(curl:*)
  - Read
  - Write
  - Edit
when_to_use: Use when the user wants to create a Poridhi lab for WebSocket Load Balancing with ALB. Examples: 'create lab 40', 'websocket ALB lab', 'poridhi lab for websockets'.
argument-hint: ""
context: inline
---

# Poridhi Lab 40: WebSocket Load Balancing with ALB

## Overview
This lab teaches how to deploy a WebSocket server behind an AWS Application Load Balancer (ALB) with proper upgrade header handling and target group stickiness for persistent connections.

## Learning Objectives
- Understand WebSocket protocol and HTTP upgrade mechanism
- Configure ALB listeners for WebSocket traffic (HTTP 80 / HTTPS 443)
- Enable target group stickiness for persistent WebSocket connections
- Deploy a FastAPI WebSocket server with proper upgrade headers
- Test and verify WebSocket connections through ALB

## Prerequisites
- AWS account with appropriate permissions
- Terraform installed
- Docker installed
- Python 3.9+
- Basic understanding of HTTP and WebSocket protocols

## Lab Structure

```
lab-40-websocket-alb/
├── README.md                          # Lab instructions and guide
├── infrastructure/
│   └── terraform/
│       ├── main.tf                    # Main Terraform configuration
│       ├── variables.tf               # Input variables
│       ├── outputs.tf                 # Output values
│       └── user_data.sh               # EC2 instance bootstrap script
├── app/
│   ├── main.py                        # FastAPI WebSocket server
│   ├── requirements.txt               # Python dependencies
│   ├── Dockerfile                     # Container image
│   └── docker-compose.yml             # Local testing setup
└── tests/
    ├── test_websocket.py              # Connection tests
    ├── test_stickiness.py             # Sticky session tests
    └── load_test.py                   # Multiple concurrent connections
```

## Architecture

```
[WebSocket Client] → [ALB:80/443] → [Target Group w/ Stickiness] → [EC2 w/ FastAPI]
```

## Quick Start

### 1. Deploy Infrastructure
```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

### 2. Build and Deploy Application
```bash
cd app
docker build -t websocket-server .
```

### 3. Test WebSocket Connection
```bash
python tests/test_websocket.py
```

## Key Concepts

### WebSocket Upgrade Headers
WebSocket connections start as HTTP requests with special headers:
- `Upgrade: websocket`
- `Connection: Upgrade`
- `Sec-WebSocket-Key: <base64-encoded-key>`
- `Sec-WebSocket-Version: 13`

### Target Group Stickiness
ALB supports two types of stickiness:
- **LBCookieStickiness**: Load balancer generated cookie (recommended)
- **AppCookieStickiness**: Application generated cookie

Duration: 1 second to 7 days (default 1 day)

### Why Stickiness Matters
WebSocket connections are long-lived. Stickiness ensures:
- All messages from a client go to the same backend
- Connection state is maintained
- Reduces overhead of re-establishing connections

## Success Criteria

1. ✅ ALB accepts WebSocket connections on port 80 and 443
2. ✅ Target group has stickiness enabled
3. ✅ FastAPI server properly handles WebSocket upgrades
4. ✅ Multiple concurrent connections are load balanced with stickiness
5. ✅ Health checks correctly identify healthy targets
6. ✅ SSL/TLS termination works for wss:// connections

## Troubleshooting

### Common Issues

**Issue: WebSocket connection fails with 400 Bad Request**
- Check that ALB listener protocol is HTTP (not HTTPS-only)
- Verify Sec-WebSocket headers are properly formatted
- Ensure target group protocol matches

**Issue: Connections not sticking to same target**
- Verify stickiness is enabled in target group
- Check cookie duration settings
- Ensure client sends cookies

**Issue: Health checks failing**
- FastAPI health endpoint returns 200 OK
- Check security group allows ALB → target traffic
- Verify target port matches application port

## Additional Resources

- AWS ALB WebSocket Documentation
- FastAPI WebSocket Documentation
- RFC 6455 - WebSocket Protocol

## Cleanup

```bash
cd infrastructure/terraform
terraform destroy
```

## Next Steps

- Lab 41: WebSocket Auto Scaling
- Lab 42: WebSocket with API Gateway
- Lab 43: WebSocket Monitoring and Observability
# Lab 40: WebSocket Load Balancing with ALB

## Overview

This lab demonstrates how to deploy a WebSocket server behind an AWS Application Load Balancer (ALB) with proper HTTP upgrade header handling and target group stickiness for persistent connections.

## Learning Objectives

By the end of this lab, you will be able to:

- ✅ Understand the WebSocket protocol and HTTP upgrade mechanism
- ✅ Configure an ALB with WebSocket listeners (HTTP 80 / HTTPS 443)
- ✅ Enable and verify target group stickiness for persistent connections
- ✅ Deploy a FastAPI WebSocket server with proper upgrade headers
- ✅ Test and troubleshoot WebSocket connections through an ALB

## Architecture

```
┌─────────────────┐
 WebSocket Client│
└────────┬────────┘
         │ ws:// or wss://
         ▼
┌─────────────────┐
│   AWS ALB       │
│  (Port 80/443)  │
│  + Stickiness   │
└────────┬────────┘
         │ HTTP (with upgrade headers)
         ▼
┌─────────────────┐
│  Target Group   │
│  (Auto Scaling) │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐ ┌────────┐
│EC2 #1  │ │EC2 #2  │
│FastAPI │ │FastAPI │
│WS App  │ │WS App  │
└─│───────┘ └────────┘
```

## Prerequisites

- AWS account with appropriate permissions
- Terraform >= 1.0
- Docker
- Python 3.9+
- AWS CLI configured

## Project Structure

```
lab-40/
├── README.md                          # This file
├── infrastructure/
│   └── terraform/
│       ├── main.tf                    # Main Terraform config
│       ├── variables.tf               # Input variables
│       ├── outputs.tf                 # Output values
│       └── user_data.sh               # EC2 bootstrap script
├── app/
│   ├── main.py                        # FastAPI WebSocket server
│   ├── requirements.txt               # Python dependencies
│   ├── Dockerfile                     # Container image
│   └── docker-compose.yml             # Local testing
└── tests/
    ├── requirements.txt               # Test dependencies
    ├── test_websocket.py              # Basic connection test
    ├── test_stickiness.py             # Sticky session test
    └── load_test.py                   # Concurrent load test
```

## Lab Steps

### Step 1: Local Testing (Optional)

Test the WebSocket server locally before deploying to AWS:

```bash
cd app
docker-compose up --build
```

In another terminal, run the test:
```bash
cd tests
pip install -r requirements.txt
python test_websocket.py ws://localhost:8000/ws
```

### Step 2: Deploy Infrastructure

Navigate to the Terraform directory and deploy the infrastructure:

```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

This will create:
- VPC and networking components
- Security groups
- Application Load Balancer
- Target group with stickiness enabled
- Auto Scaling Group with EC2 instances
- ALB listeners on ports 80 and 443

### Step 3: Verify Deployment

After Terraform completes, check the outputs:

```bash
terraform output
```

You should see:
- `alb_dns_name`: DNS name of your ALB
- `websocket_url_http`: WebSocket URL over HTTP
- `websocket_url_https`: WebSocket URL over HTTPS

### Step 4: Test WebSocket Connection

Test the basic connection through the ALB:

```bash
cd tests
python test_websocket.py ws://YOUR-ALB-DNS/ws
```

### Step 5: Verify Stickiness

Run the stickiness test to verify connections stay on the same backend:

```bash
python test_stickiness.py ws://YOUR-ALB-DNS/ws
```

Expected output: All connections should consistently route to the same backend instance.

### Step 6: Load Testing

Test with multiple concurrent connections:

```bash
python load_test.py ws://YOUR-ALB-DNS/ws 50 30
```

This creates 50 concurrent clients for 30 seconds.

## Key Concepts

### 1. WebSocket Upgrade Headers

WebSocket connections start as HTTP requests with special headers:

```http
GET /ws HTTP/1.1
Host: example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

FastAPI handles these automatically when you use `@app.websocket()` decorator.

### 2. ALB WebSocket Support

ALB supports WebSocket natively when:
- Listener protocol is HTTP or HTTPS
- Target group protocol is HTTP
- Backend properly handles upgrade headers

### 3. Target Group Stickiness

Two types of stickiness:
- **lb_cookie**: Load balancer generated cookie (recommended)
- **app_cookie**: Application generated cookie

Configuration in Terraform:
```hcl
stickiness {
  type            = "lb_cookie"
  cookie_duration = 86400  # 1 day in seconds
  enabled          = true
}
```

### 4. Why Stickiness Matters

WebSocket connections are long-lived. Stickiness ensures:
- All messages from a client go to the same backend
- Connection state is maintained
- No reconnection overhead
- Better resource utilization

## Testing Scenarios

### Scenario 1: Basic Connection
- Establish single WebSocket connection
- Send and receive messages
- Verify echo functionality

### Scenario 2: Stickiness Verification
- Create multiple connections
- Send multiple messages per connection
- Verify all messages route to same backend

### Scenario 3: Load Testing
- Simulate 20-50 concurrent clients
- Measure throughput and latency
- Verify load distribution

## Troubleshooting

### Issue: WebSocket connection returns 400 Bad Request

**Cause**: ALB cannot parse WebSocket upgrade headers

**Solution**:
- Verify listener protocol is HTTP (not TCP)
- Check that FastAPI properly handles upgrade
- Review CloudWatch logs for ALB

### Issue: Connections not sticking to same target

**Cause**: Stickiness not enabled or cookies not being sent

**Solution**:
- Verify `stickiness` block in target group
- Check cookie duration (must be > 0)
- Ensure client sends cookies back to ALB

### Issue: Health checks failing

**Cause**: Application not responding on health endpoint

**Solution**:
- Verify `/health` endpoint returns 200
- Check security group allows ALB → target traffic
- Review health check configuration in target group

### Issue: HTTPS connection fails

**Cause**: SSL certificate issues

**Solution**:
- For testing, use self-signed certificate
- For production, use ACM certificate
- Verify certificate ARN in HTTPS listener

## Monitoring

### CloudWatch Metrics

Monitor these key metrics:
- `ActiveConnectionCount`: Current WebSocket connections
- `NewConnectionCount`: New connections per minute
- `TargetResponseTime`: Backend response time
- `HealthyHostCount`: Healthy targets
- `UnHealthyHostCount`: Unhealthy targets

### Application Logs

FastAPI logs include:
- Connection events
- Message exchanges
- Error details

Access logs via:
```bash
# SSH to instance
ssh ec2-user@INSTANCE_IP

# View application logs
sudo journalctl -u websocket-app -f
```

## Cleanup

To avoid ongoing charges, destroy the infrastructure:

```bash
cd infrastructure/terraform
terraform destroy
```

## Additional Resources

- [AWS ALB WebSocket Documentation](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-websockets.html)
- [FastAPI WebSocket Documentation](https://fastapi.tiangolo.com/advanced/websockets/)
- [RFC 6455 - The WebSocket Protocol](https://tools.ietf.org/html/rfc6455)
- [Terraform AWS Provider Documentation](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)

## Success Criteria Checklist

- [ ] ALB accepts WebSocket connections on port 80
- [ ] ALB accepts WebSocket connections on port 443 (HTTPS)
- [ ] Target group has stickiness enabled
- [ ] FastAPI server properly handles WebSocket upgrades
- [ ] Multiple concurrent connections work correctly
- [ ] Connections stick to same backend instance
- [ ] Health checks correctly identify healthy targets
- [ ] SSL/TLS termination works for wss:// connections
- [ ] All test scripts pass successfully

## Support

If you encounter issues:
1. Check the troubleshooting section above
2. Review CloudWatch logs
3. Verify Terraform configuration
4. Test locally with Docker before deploying

## Conclusion

This lab taught you to deploy WebSocket servers behind AWS ALB with proper upgrade headers and stickiness. You configured HTTP/HTTPS listeners, enabled session persistence, and built a FastAPI WebSocket application. The infrastructure uses Auto Scaling for high availability and includes testing scripts for verification. You now have the foundation to build scalable real-time applications.
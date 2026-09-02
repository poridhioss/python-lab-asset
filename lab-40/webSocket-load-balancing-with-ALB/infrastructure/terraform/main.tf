###############################################################################
# Lab 40: WebSocket Load Balancing with ALB
# Terraform configuration for ALB with WebSocket support and target group stickiness
###############################################################################

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

###############################################################################
# Data Sources
###############################################################################

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
}

###############################################################################
# Networking
###############################################################################

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "lab40-vpc"
  }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.${count.index + 1}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = {
    Name = "lab40-public-subnet-${count.index + 1}"
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "lab40-igw"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "lab40-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

###############################################################################
# Security Groups
###############################################################################

# ALB Security Group - Allow HTTP/HTTPS from anywhere
resource "aws_security_group" "alb" {
  name        = "lab40-alb-sg"
  description = "Security group for ALB"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTP from internet"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS from internet"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "lab40-alb-sg"
  }
}

# EC2 Security Group - Allow traffic from ALB only
resource "aws_security_group" "web" {
  name        = "lab40-web-sg"
  description = "Security group for WebSocket servers"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "HTTP from ALB"
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  ingress {
    description = "SSH from anywhere"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "lab40-web-sg"
  }
}

###############################################################################
# IAM Role for EC2
###############################################################################

resource "aws_iam_role" "ec2_role" {
  name = "lab40-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = "lab40-ec2-profile"
  role = aws_iam_role.ec2_role.name
}

###############################################################################
# Launch Template
###############################################################################

resource "aws_launch_template" "websocket" {
  name_prefix   = "lab40-websocket-"
  image_id      = data.aws_ami.amazon_linux_2.id
  instance_type = var.instance_type

  vpc_security_group_ids = [aws_security_group.web.id]

  iam_instance_profile {
    name = aws_iam_instance_profile.ec2_profile.name
  }

  user_data = base64encode(templatefile("${path.module}/user_data.sh", {
    instance_id = "${aws_lb.main.name}-${count.index}"
  }))

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name = "lab40-websocket-server"
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

###############################################################################
# Auto Scaling Group
###############################################################################

resource "aws_autoscaling_group" "websocket" {
  name                = "lab40-websocket-asg"
  vpc_zone_identifier = aws_subnet.public[*].id
  target_group_arns   = [aws_lb_target_group.websocket.arn]
  health_check_type   = "ELB"
  health_check_grace_period = 300
  min_size            = var.min_instances
  max_size            = var.max_instances
  desired_capacity    = var.desired_instances

  launch_template {
    id      = aws_launch_template.websocket.id
    version = "$Latest"
  }

  tag {
    key                 = "Name"
    value               = "lab40-websocket-server"
    propagate_at_launch = true
  }
}

###############################################################################
# Application Load Balancer
###############################################################################

resource "aws_lb" "main" {
  name               = "lab40-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id

  enable_deletion_protection = false

  tags = {
    Name = "lab40-alb"
  }
}

###############################################################################
# Target Group with Stickiness
###############################################################################

resource "aws_lb_target_group" "websocket" {
  name        = "lab40-ws-tg"
  port        = 8000
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "instance"

  # Health check configuration
  health_check {
    enabled             = true
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 5
    interval            = 30
    path                = "/health"
    matcher             = "200"
    port                = "8000"
  }

  # Stickiness configuration for WebSocket connections
  stickiness {
    type            = "lb_cookie"
    cookie_duration  = 86400  # 1 day
    enabled          = true
  }

  # Connection settings for long-lived WebSocket connections
  deregistration_delay = 30

  tags = {
    Name = "lab40-websocket-tg"
  }
}

###############################################################################
# ALB Listeners (HTTP 80 and HTTPS 443)
###############################################################################

# HTTP Listener (Port 80)
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.main.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.websocket.arn
  }

  tags = {
    Name = "lab40-http-listener"
  }
}

# HTTPS Listener (Port 443)
resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.main.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-2016-08"
  certificate_arn   = var.ssl_certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.websocket.arn
  }

  tags = {
    Name = "lab40-https-listener"
  }
}

###############################################################################
# Self-Signed SSL Certificate (for testing only)
###############################################################################

resource "aws_acm_certificate" "self_signed" {
  count            = var.create_self_signed_cert ? 1 : 0
  private_key      = tls_private_key.cert[0].private_key_pem
  certificate_body = tls_self_signed_cert.cert[0].cert_pem

  subject {
    common_name  = var.domain_name
    organization = "Poridhi Lab 40"
  }

  validity_period_hours = 24

  lifecycle {
    create_before_destroy = true
  }
}

resource "tls_private_key" "cert" {
  count     = var.create_self_signed_cert ? 1 : 0
  algorithm = "RSA"
  rsa_bits  = 2048
}

resource "tls_self_signed_cert" "cert" {
  count           = var.create_self_signed_cert ? 1 : 0
  private_key_pem = tls_private_key.cert[0].private_key_pem

  subject {
    common_name  = var.domain_name
    organization = "Poridhi Lab 40"
  }

  validity_period_hours = 24

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
  ]
}
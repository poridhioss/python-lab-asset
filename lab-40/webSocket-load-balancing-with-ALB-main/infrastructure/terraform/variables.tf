###############################################################################
# Lab 40: Variable Definitions
###############################################################################

variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type for WebSocket servers"
  type        = string
  default     = "t2.micro"
}

variable "min_instances" {
  description = "Minimum number of instances in ASG"
  type        = number
  default     = 2
}

variable "max_instances" {
  description = "Maximum number of instances in ASG"
  type        = number
  default     = 4
}

variable "desired_instances" {
  description = "Desired number of instances in ASG"
  type        = number
  default     = 2
}

variable "domain_name" {
  description = "Domain name for SSL certificate"
  type        = string
  default     = "lab40.example.com"
}

variable "ssl_certificate_arn" {
  description = "ARN of SSL certificate in ACM (leave empty to create self-signed)"
  type        = string
  default     = ""
}

variable "create_self_signed_cert" {
  description = "Whether to create a self-signed certificate for testing"
  type        = bool
  default     = true
}
###############################################################################
# Lab 40: Output Values
###############################################################################

output "alb_dns_name" {
  description = "DNS name of the Application Load Balancer"
  value       = aws_lb.main.dns_name
}

output "alb_arn" {
  description = "ARN of the Application Load Balancer"
  value       = aws_lb.main.arn
}

output "target_group_arn" {
  description = "ARN of the WebSocket target group"
  value       = aws_lb_target_group.websocket.arn
}

output "target_group_name" {
  description = "Name of the WebSocket target group"
  value       = aws_lb_target_group.websocket.name
}

output "http_listener_arn" {
  description = "ARN of the HTTP listener"
  value       = aws_lb_listener.http.arn
}

output "https_listener_arn" {
  description = "ARN of the HTTPS listener"
  value       = aws_lb_listener.https.arn
}

output "websocket_url_http" {
  description = "WebSocket URL over HTTP"
  value       = "ws://${aws_lb.main.dns_name}/ws"
}

output "websocket_url_https" {
  description = "WebSocket URL over HTTPS"
  value       = "wss://${aws_lb.main.dns_name}/ws"
}

output "instance_ids" {
  description = "IDs of EC2 instances in the ASG"
  value       = aws_autoscaling_group.websocket.instances[*].id
}

output "test_page_url" {
  description = "URL for the WebSocket test page"
  value       = "http://${aws_lb.main.dns_name}/ws-test"
}

output "self_signed_cert_arn" {
  description = "ARN of self-signed certificate (if created)"
  value       = var.create_self_signed_cert ? aws_acm_certificate.self_signed[0].arn : null
}
variable "aws_region" {
  description = "AWS Region to deploy to"
  default     = "us-east-1"
}
variable "db_user" {
  description = "PostgreSQL Username"
  type        = string
}
variable "db_password" {
  description = "PostgreSQL Password"
  type        = string
  sensitive   = true
}

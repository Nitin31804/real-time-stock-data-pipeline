# AWS Infrastructure as Code for Real-Time Stock Pipeline
provider "aws" {
  region = var.aws_region
}

# VPC Network
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  name   = "stock-pipeline-vpc"
  cidr   = "10.0.0.0/16"
  azs             = ["us-east-1a", "us-east-1b", "us-east-1c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
  enable_nat_gateway = true
}

# Amazon MSK (Managed Streaming for Apache Kafka)
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "stock-pipeline-msk"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.large"
    client_subnets = module.vpc.private_subnets
    security_groups = [aws_security_group.msk_sg.id]
  }
}

# Amazon RDS (TimescaleDB / PostgreSQL)
resource "aws_db_instance" "postgres" {
  identifier           = "stock-pipeline-db"
  allocated_storage    = 50
  engine               = "postgres"
  engine_version       = "15.3"
  instance_class       = "db.t3.medium"
  db_name              = "stock_db"
  username             = var.db_user
  password             = var.db_password
  skip_final_snapshot  = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name   = module.vpc.database_subnet_group
}

# ECS Cluster for Flask Dashboard & Spark Jobs
resource "aws_ecs_cluster" "main" {
  name = "stock-pipeline-ecs"
}

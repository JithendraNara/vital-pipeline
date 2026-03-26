# infrastructure/main.tf
# Healthcare Data Platform — AWS Infrastructure
# Provisions: RDS PostgreSQL + S3 + ECS Fargate (Airflow)
# HIPAA-conscious configuration (encryption at rest, VPC, no public RDS)

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }

  backend "s3" {
    bucket = "vital-pipeline-terraform-state"
    key    = "prod/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ============================================================
# Variables
# ============================================================

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.medium"
}

variable "db_allocated_storage" {
  description = "RDS allocated storage in GB"
  type        = number
  default     = 50
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access resources"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

# ============================================================
# Random ID for uniqueness
# ============================================================

resource "random_id" "suffix" {
  byte_length = 4
}

# ============================================================
# VPC — private network for healthcare data
# ============================================================

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name        = "vital-pipeline-vpc"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_subnet" "private_a" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone        = "${var.aws_region}a"
  map_public_ip_on_launch = false

  tags = {
    Name = "vital-pipeline-private-a"
  }
}

resource "aws_subnet" "private_b" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.2.0/24"
  availability_zone        = "${var.aws_region}b"
  map_public_ip_on_launch = false

  tags = {
    Name = "vital-pipeline-private-b"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.0.0/24"
  availability_zone        = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "vital-pipeline-public"
  }
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "vital-pipeline-igw" }
}

# Route table (public)
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }
  tags = { Name = "vital-pipeline-public-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# NAT Gateway (for private subnets to access internet)
resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }
  tags = { Name = "vital-pipeline-private-rt" }
}

resource "aws_route_table_association" "private_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}

resource "aws_route_table_association" "private_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

# ============================================================
# RDS PostgreSQL — Primary Data Warehouse
# ============================================================

resource "aws_security_group" "rds" {
  name        = "vital-pipeline-rds-sg"
  description = "Security group for RDS PostgreSQL"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]  # Only from within VPC
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "vital-pipeline-rds-sg"
    Environment = var.environment
  }
}

resource "aws_db_subnet_group" "main" {
  name       = "vital-pipeline-db-subnet"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]

  tags = { Name = "vital-pipeline-db-subnet" }
}

resource "aws_db_instance" "postgres" {
  identifier           = "vital-pipeline-db-${random_id.suffix.hex}"
  engine              = "postgres"
  engine_version      = "15.4"
  instance_class      = var.db_instance_class
  allocated_storage   = var.db_allocated_storage
  storage_encrypted   = true                    # HIPAA: encryption at rest
  storage_type        = "gp3"

  db_name  = "vitalpipeline"
  username = "atlas"
  password = random_password.db_password.result

  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  # Backup — HIPAA requires 90-day retention
  backup_retention_period = 90
  backup_window           = "03:00-04:00"
  maintenance_window      = "Mon:04:00-Mon:05:00"

  # High availability
  multi_az               = true
  deletion_protection    = true  # Prevent accidental deletion

  # Logging (audit trail for HIPAA)
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  publicly_accessible = false  # HIPAA: no public access

  tags = {
    Name        = "vital-pipeline-postgres"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "random_password" "db_password" {
  length  = 32
  special = true
}

# ============================================================
# S3 — Data Lake (Bronze/Silver/Gold)
# ============================================================

resource "aws_security_group" "s3_endpoint" {
  name        = "vital-pipeline-s3-endpoint-sg"
  description = "Security group for S3 VPC endpoint"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "vital-pipeline-s3-endpoint-sg" }
}

# S3 VPC Endpoint (private connectivity to S3)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name     = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids   = [aws_route_table.private.id]

  tags = { Name = "vital-pipeline-s3-endpoint" }
}

# Bronze Layer — raw Synthea/FHIR/claims data
resource "aws_s3_bucket" "bronze" {
  bucket = "vital-pipeline-bronze-${random_id.suffix.hex}"

  tags = {
    Name        = "vital-pipeline-bronze"
    Layer       = "Bronze"
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Silver Layer — standardized/cleansed data
resource "aws_s3_bucket" "silver" {
  bucket = "vital-pipeline-silver-${random_id.suffix.hex}"

  tags = {
    Name        = "vital-pipeline-silver"
    Layer       = "Silver"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Gold Layer — analytics-ready data
resource "aws_s3_bucket" "gold" {
  bucket = "vital-pipeline-gold-${random_id.suffix.hex}"

  tags = {
    Name        = "vital-pipeline-gold"
    Layer       = "Gold"
    Environment = var.environment
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ============================================================
# Secrets Manager — HIPAA-compliant credential storage
# ============================================================

resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "vital-pipeline/db-credentials"
  description = "Database credentials for vital-pipeline data platform"

  recovery_window_in_days = 90  # HIPAA: cannot delete within 90 days

  tags = {
    Name        = "vital-pipeline-db-credentials"
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    username = "atlas"
    password = random_password.db_password.result
    engine   = "postgres"
    host     = aws_db_instance.postgres.address
    port     = 5432
    dbname   = "vitalpipeline"
  })
}

# ============================================================
# ECS Fargate — Airflow on AWS
# ============================================================

resource "aws_security_group" "ecs" {
  name        = "vital-pipeline-ecs-sg"
  description = "Security group for ECS Fargate tasks"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Airflow web UI — restrict in production
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "vital-pipeline-ecs-sg" }
}

resource "aws_ecs_cluster" "main" {
  name = "vital-pipeline-cluster"

  setting {
    name  = "awsvpcConfiguration"
    value = "enableTransitEncryption=false"
  }

  tags = {
    Name        = "vital-pipeline-ecs-cluster"
    Environment = var.environment
  }
}

resource "aws_ecs_task_definition" "airflow" {
  family                   = "vital-pipeline-airflow"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "2048"    # 2 vCPU
  memory                   = "8192"    # 8 GB
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "airflow-webserver"
      image     = "apache/airflow:2.8.1-python3.11"
      essential = true
      portMappings = [{ containerPort = 8080 }]
      environment = [
        { name = "AIRFLOW__CORE__EXECUTOR", value = "CeleryExecutor" }
        { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", value = "postgresql+psycopg2://${aws_db_instance.postgres.username}:<password>@${aws_db_instance.postgres.address}:5432/vitalpipeline" }
        { name = "AIRFLOW__CORE__FERNET_KEY", value = "<generate-with-openssl-rand-base64-32>" }
      ]
      secrets = [
        { name = "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", valueFrom = "${aws_secretsmanager_secret.db_credentials.arn}:password::" }
      ]
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = "/ecs/vital-pipeline"
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "airflow"
        }
      }
    }
  ])

  tags = {
    Name        = "vital-pipeline-airflow-task"
    Environment = var.environment
  }
}

# IAM Roles for ECS
resource "aws_iam_role" "ecs_task_execution" {
  name = "vital-pipeline-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_execution" {
  name = "vital-pipeline-ecs-task-execution-policy"
  role = aws_iam_role.ecs_task_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role" "ecs_task" {
  name = "vital-pipeline-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task" {
  name = "vital-pipeline-ecs-task-policy"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "rds-db:connect"  # IAM auth to RDS
        ]
        Resource = [
          aws_s3_bucket.bronze.arn,
          aws_s3_bucket.silver.arn,
          aws_s3_bucket.gold.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          "${aws_s3_bucket.silver.arn}/*",
          "${aws_s3_bucket.gold.arn}/*",
          aws_db_instance.postgres.arn
        ]
      }
    ]
  })
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "ecs" {
  name              = "/ecs/vital-pipeline"
  retention_in_days = 90  # HIPAA: 90-day log retention

  tags = {
    Name        = "vital-pipeline-ecs-logs"
    Environment = var.environment
  }
}

# ============================================================
# Outputs
# ============================================================

output "rds_endpoint" {
  description = "PostgreSQL connection endpoint"
  value       = aws_db_instance.postgres.endpoint
  sensitive   = true
}

output "rds_arn" {
  description = "PostgreSQL RDS ARN"
  value       = aws_db_instance.postgres.arn
}

output "s3_bronze_bucket" {
  description = "Bronze layer S3 bucket name"
  value       = aws_s3_bucket.bronze.bucket
}

output "s3_silver_bucket" {
  description = "Silver layer S3 bucket name"
  value       = aws_s3_bucket.silver.bucket
}

output "s3_gold_bucket" {
  description = "Gold layer S3 bucket name"
  value       = aws_s3_bucket.gold.bucket
}

output "ecs_cluster" {
  description = "ECS Fargate cluster name"
  value       = aws_ecs_cluster.main.name
}

output "airflow_webserver_url" {
  description = "Airflow webserver URL (after deployment)"
  value       = "http://${aws_lb.airflow.public_ip}:8080"
}

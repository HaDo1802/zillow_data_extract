terraform {
  required_version = ">= 1.5"
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

resource "aws_key_pair" "airflow" {
  key_name   = "airflow-ec2-key"
  public_key = file(var.public_key_path)
}

resource "aws_instance" "airflow" {
  ami                    = var.ubuntu_ami
  instance_type          = "t2.micro"
  key_name               = aws_key_pair.airflow.key_name
  vpc_security_group_ids = [aws_security_group.airflow_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.airflow.name

  root_block_device {
    volume_size = 20
    volume_type = "gp2"
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    repo_url = var.repo_url
  })

  tags = {
    Name    = "airflow-etl-server"
    Project = "real-estate-etl"
  }
}

resource "aws_eip" "airflow" {
  instance = aws_instance.airflow.id
  domain   = "vpc"
}

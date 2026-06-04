data "external" "my_ip" {
  program = ["bash", "-c", "printf '{\"ip\":\"%s/32\"}' $(curl -s https://checkip.amazonaws.com)"]
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_key_pair" "airflow" {
  key_name   = "airflow-ec2-key"
  public_key = file(var.public_key_path)
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.ubuntu.id
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

#!/bin/bash
set -euo pipefail

# Install Docker
curl -fsSL https://get.docker.com | sh
usermod -aG docker ubuntu

# 2 GB swap — t2.micro has 1 GB RAM, not enough for running Airflow + Postgres
# in production, using bigger ec2 instances would not require swap, but for free testing purposes it is needed
fallocate -l 2G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# Clone repo — docker-compose.yaml is needed; image is pulled from Docker Hub by CI/CD
sudo -u ubuntu git clone "${repo_url}" /home/ubuntu/real_estate_extract

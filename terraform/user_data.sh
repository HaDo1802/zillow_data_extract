#!/bin/bash
# Bootstrap script for the Airflow ETL EC2 instance.
# Runs once at first launch. Installs Docker, sets up swap, clones the repo.
# The .env file and docker compose startup are handled by the first CI/CD deploy.
set -euo pipefail
exec > >(tee /var/log/user-data.log) 2>&1

echo "=== Airflow ETL server bootstrap starting ==="

# System update
apt-get update -y
apt-get upgrade -y

# Install Docker via official repo (apt's version is outdated)
apt-get install -y ca-certificates curl gnupg lsb-release git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  | tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

systemctl enable docker
systemctl start docker
usermod -aG docker ubuntu

# Add 2 GB swap — mandatory on t2.micro (1 GB RAM is not enough for Airflow + Postgres)
if [ ! -f /swapfile ]; then
  fallocate -l 2G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
  echo "Swap: 2 GB enabled"
fi

# Clone the repo as the ubuntu user
REPO_URL="${repo_url}"
sudo -u ubuntu git clone "$REPO_URL" /home/ubuntu/real_estate_extract

echo "=== Bootstrap complete ==="
echo ""
echo "Next steps:"
echo "  1. Add EC2_HOST, EC2_SSH_KEY, and EC2_ENV_FILE to your GitHub repository secrets."
echo "  2. Trigger the CI/CD workflow (workflow_dispatch) from the GitHub Actions tab."
echo "  3. The deploy job will SCP your .env and start docker compose automatically."
echo ""
echo "Bootstrap log: /var/log/user-data.log"

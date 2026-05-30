variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "ubuntu_ami" {
  description = "Ubuntu 22.04 LTS AMI ID (free tier eligible). Find the current AMI for your region at: https://cloud-images.ubuntu.com/locator/ec2/"
  type        = string
  # us-east-1: Ubuntu 22.04 LTS (HVM), SSD Volume Type — verify this is still current before apply
  default = "ami-0c7217cdde317cfec"
}

variable "public_key_path" {
  description = "Path to your SSH public key file (e.g. ~/.ssh/id_ed25519.pub)"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "allowed_ssh_cidr" {
  description = "Your IP address in CIDR notation for SSH and Airflow UI access (e.g. 1.2.3.4/32). Find yours at https://checkip.amazonaws.com"
  type        = string
}

variable "repo_url" {
  description = "HTTPS URL of the Git repository to clone on the EC2 instance"
  type        = string
  default     = "https://github.com/YOUR_GITHUB_USERNAME/real_estate_extract.git"
}

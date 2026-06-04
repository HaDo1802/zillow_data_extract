variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-southeast-1"
}

variable "public_key_path" {
  description = "Path to your SSH public key file (e.g. ~/.ssh/id_ed25519.pub)"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

variable "repo_url" {
  description = "HTTPS URL of the public GitHub repository to clone on the EC2 instance"
  type        = string
  default     = "https://github.com/HaDo1802/zillow_data_extract.git"
}

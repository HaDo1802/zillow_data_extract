output "ec2_public_ip" {
  description = "Elastic IP address of the Airflow server"
  value       = aws_eip.airflow.public_ip
}

output "ssh_command" {
  description = "SSH command to connect to the Airflow server"
  value       = "ssh ubuntu@${aws_eip.airflow.public_ip}"
}

output "airflow_ui_url" {
  description = "URL to access the Airflow web UI (open in browser after docker compose is up)"
  value       = "http://${aws_eip.airflow.public_ip}:8080"
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "airflow-etl-instance-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = {
    Project = "real-estate-etl"
  }
}

# SSM Session Manager: browser-based terminal from AWS console — useful if SSH key is lost
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "airflow" {
  name = "airflow-etl-instance-profile"
  role = aws_iam_role.airflow.name
}

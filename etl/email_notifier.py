"""
Email Notification Module for ETL Pipeline
==========================================

Sends email notifications about ETL pipeline success/failure status.
"""

import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv
from logger import get_logger

logger = get_logger(__name__)

load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
)


class EmailNotifier:
    """Email notification service for ETL pipeline results."""

    def __init__(self):
        """Initialize email configuration from environment variables."""
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.sender_password = os.getenv("SENDER_PASSWORD")
        self.recipient_email = os.getenv("RECIPIENT_EMAIL", "havando1802@gmail.com")

        logger.info(
            f"Email notifier initialized: {self.sender_email} -> {self.recipient_email}"
        )

    def send_notification(self, success: bool, details: dict = None):
        """Send email notification about ETL pipeline result."""
        if not self.sender_email or not self.sender_password:
            logger.error(
                "Email credentials not configured (SENDER_EMAIL or SENDER_PASSWORD missing)"
            )
            logger.error("Skipping email notification")
            return False

        try:
            logger.info(
                f"Preparing {'success' if success else 'failure'} email notification..."
            )

            message = MIMEMultipart()
            message["From"] = self.sender_email
            message["To"] = self.recipient_email

            if success:
                subject = "✅ Real Estate ETL Pipeline - Success"
                body = self._create_success_email_body(details)
            else:
                subject = "❌ Real Estate ETL Pipeline - Failed"
                body = self._create_failure_email_body(details)

            message["Subject"] = subject
            message.attach(MIMEText(body, "html"))

            logger.info(
                f"Connecting to SMTP server: {self.smtp_server}:{self.smtp_port}"
            )
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.send_message(message)

            logger.info(
                f"Email notification sent successfully to {self.recipient_email}"
            )
            return True

        except smtplib.SMTPAuthenticationError:
            logger.error(
                "SMTP authentication failed - check SENDER_EMAIL and SENDER_PASSWORD"
            )
            return False
        except smtplib.SMTPException as e:
            logger.error(f"SMTP error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Failed to send email notification: {str(e)}", exc_info=True)
            return False

    def _create_success_email_body(self, details: dict = None) -> str:
        """Create HTML email body for successful pipeline run."""
        details = details or {}

        html_body = (
            "<html>"
            "<head>"
            "<style>"
            "body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }"
            ".header { background-color: #28a745; color: white; padding: 20px; border-radius: 5px; }"
            ".content { padding: 20px; }"
            ".metric { background-color: #f8f9fa; padding: 15px; margin: 10px 0; "
            "border-left: 4px solid #28a745; }"
            ".metric-label { font-weight: bold; color: #666; }"
            ".metric-value { font-size: 18px; color: #28a745; }"
            ".footer { font-size: 12px; color: #666; margin-top: 30px; padding-top: 20px; "
            "border-top: 1px solid #ddd; }"
            "</style>"
            "</head>"
            "<body>"
            "<div class='header'><h2>🎉 Real Estate ETL Pipeline Completed Successfully!</h2></div>"
            "<div class='content'>"
            f"<p><strong>ETL Run ID:</strong> {details.get('etl_run_id', 'N/A')}</p>"
            f"<p><strong>Execution Time:</strong> {details.get('end_time', 'N/A')}</p>"
            "<h3>Pipeline Metrics:</h3>"
            "<div class='metric'><div class='metric-label'>Properties Extracted</div>"
            f"<div class='metric-value'>{details.get('properties_extracted', 'N/A')}</div></div>"
            "<div class='metric'><div class='metric-label'>Records Loaded to Database</div>"
            f"<div class='metric-value'>{details.get('records_loaded', 'N/A')}</div></div>"
            "<div class='metric'><div class='metric-label'>Data Quality Pass Rate</div>"
            f"<div class='metric-value'>{details.get('quality_rate', 'N/A')}</div></div>"
            "<div class='metric'><div class='metric-label'>Total Duration</div>"
            f"<div class='metric-value'>{details.get('duration', 'N/A')}</div></div>"
            "<h3>Next Steps:</h3>"
            "<p>Your real estate data has been successfully extracted, transformed, and loaded.</p>"
            "<pre style='background-color: #f8f9fa; padding: 10px; border-radius: 5px;'>"
            "SELECT COUNT(*) FROM real_estate_data.properties_data_history;\n"
            "SELECT * FROM real_estate_data.properties_data_current LIMIT 10;"
            "</pre>"
            "</div>"
            "<div class='footer'>"
            "<p>This is an automated message from your Real Estate ETL Pipeline.</p>"
            f"<p>Environment: {details.get('environment', 'N/A')}</p>"
            "</div>"
            "</body>"
            "</html>"
        )
        return html_body

    def _create_failure_email_body(self, details: dict = None) -> str:
        """Create HTML email body for failed pipeline run."""
        details = details or {}

        # Build optional properties extracted block safely
        properties_block = (
            f"<p><strong>Properties Extracted:</strong> {details.get('properties_extracted', 'N/A')}</p>"
            if "properties_extracted" in details
            else ""
        )

        html_body = (
            "<html>"
            "<head>"
            "<style>"
            "body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }"
            ".header { background-color: #dc3545; color: white; padding: 20px; border-radius: 5px; }"
            ".content { padding: 20px; }"
            ".error-box { background-color: #f8d7da; border: 1px solid #dc3545; padding: 15px; "
            "margin: 15px 0; border-radius: 5px; }"
            ".info { background-color: #f8f9fa; padding: 15px; margin: 10px 0; "
            "border-left: 4px solid #dc3545; }"
            ".footer { font-size: 12px; color: #666; margin-top: 30px; padding-top: 20px; "
            "border-top: 1px solid #ddd; }"
            "ol { line-height: 2; }"
            "</style>"
            "</head>"
            "<body>"
            "<div class='header'><h2>⚠️ Real Estate ETL Pipeline Failed</h2></div>"
            "<div class='content'>"
            f"<p><strong>ETL Run ID:</strong> {details.get('etl_run_id', 'N/A')}</p>"
            f"<p><strong>Execution Time:</strong> {details.get('start_time', 'N/A')}</p>"
            f"<p><strong>Duration Before Failure:</strong> {details.get('total_execution_time', 'N/A')}</p>"
            "<div class='error-box'>"
            f"<p><strong>Failed Step:</strong> {details.get('failed_step', 'UNKNOWN')}</p>"
            "<p><strong>Error Message:</strong></p>"
            f"<p style='font-family: monospace; color: #721c24;'>"
            f"{details.get('error', 'Unknown error occurred')}</p>"
            "</div>"
            "<div class='info'>"
            f"<p><strong>Environment:</strong> {details.get('environment', 'N/A')}</p>"
            f"{properties_block}"
            "</div>"
            "<h3>🔧 Troubleshooting Steps:</h3>"
            "<ol>"
            "<li>Check the pipeline logs: <code>etl/log.txt</code></li>"
            "<li>Verify API key is valid in <code>.env</code></li>"
            "<li>Ensure database connection is working</li>"
            "<li>Check internet connectivity</li>"
            "<li>Verify Zillow API is responding</li>"
            "</ol>"
            "<p style='color: #dc3545; font-weight: bold;'>⚠️ Action Required: Please investigate.</p>"
            "</div>"
            "<div class='footer'>"
            "<p>This is an automated message from your Real Estate ETL Pipeline.</p>"
            "<p>If this issue persists, review the error logs and configuration settings.</p>"
            "</div>"
            "</body>"
            "</html>"
        )
        return html_body


def send_test_email():
    """Send a test email to verify configuration."""
    logger.info("Starting test email...")

    notifier = EmailNotifier()
    test_details = {
        "etl_run_id": "20241209_TEST",
        "properties_extracted": 125,
        "records_loaded": 120,
        "quality_rate": "96.0%",
        "duration": "00:02:15",
        "end_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "environment": "Local (Test)",
    }

    logger.info("Sending test success email...")
    success = notifier.send_notification(success=True, details=test_details)

    if success:
        logger.info("Test email sent successfully")
    else:
        logger.error("Failed to send test email")


if __name__ == "__main__":
    send_test_email()

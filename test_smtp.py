import os, smtplib
from email.mime.text import MIMEText

host = os.environ["HOLDIQ_SMTP_HOST"]
port = int(os.environ["HOLDIQ_SMTP_PORT"])
user = os.environ["HOLDIQ_SMTP_USER"]
pw   = os.environ["HOLDIQ_SMTP_PASS"]
from_email = os.environ.get("HOLDIQ_FROM_EMAIL", user)
to_email   = from_email  # send to yourself for testing

msg = MIMEText("Test email from HoldIQ SMTP setup.")
msg["Subject"] = "HoldIQ SMTP Test"
msg["From"] = from_email
msg["To"] = to_email

print(f"Connecting to {host}:{port} as {user}...")
with smtplib.SMTP(host, port) as server:
    server.starttls()
    server.login(user, pw)
    server.send_message(msg)
print("✅ Test email sent.")

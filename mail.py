import smtplib
from email.message import EmailMessage


def create_email(body):
    msg = EmailMessage()
    msg["From"] = "me"
    msg["To"] = "you"
    msg["Subject"] = "Paper"
    msg["Body"] = body
    return msg


def send_email(msg):
    # Send the message via our own SMTP server.
    s = smtplib.SMTP("localhost")
    s.send_message(msg)
    s.quit()

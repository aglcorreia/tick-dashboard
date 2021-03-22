import smtplib
import ssl
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email_oauth2 import get_oauth_token_and_update_config


def create_email_message(
        sender_email: str,
        receiver_email: str,
        date_to_use: str
) -> MIMEMultipart:
    # Create contents of the message
    text = f"""\
    Hi,
    Here is your daily portfolio dashboard for {date_to_use}."""
    global_kpis = open("html_outputs/portfolio_global_kpis.html", 'r').read()
    indirect = open("html_outputs/portfolio_indirect_positions.html", 'r').read()

    html_part = MIMEMultipart(_subtype='related')
    body = MIMEText(f'{text} <br> {global_kpis} <br> {indirect}', _subtype='html')
    html_part.attach(body)

    filename = 'html_outputs/portfolio_with_kpis.html'
    with open(filename, "rb") as attachment:
        attach_part = MIMEBase("application", "octet-stream")
        attach_part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(attach_part)

    # Add header as key/value pair to attachment part
    attach_part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )

    # Create email message
    message = MIMEMultipart("related")
    message["Subject"] = f"{date_to_use} portfolio dashboard"
    message["From"] = sender_email
    message["To"] = receiver_email
    message.attach(html_part)  # Attach the raw MIMEBase descendant. This is a public method on EmailMessage
    message.attach(attach_part)

    return message


def send_email_message(
        message: MIMEMultipart,
        sender_email: str,
        receiver_email: str,
        password: str,
        port: int = 465,
        smtp_server: str = "smtp.gmail.com"
):
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.quit()


def send_email_message_oauth2(
        message: MIMEMultipart,
        sender_email: str,
        receiver_email: str,
        google_client_id: str,
        google_client_secret: str,
        google_refresh_token: str = None,
        port: int = 587,
        smtp_server: str = "smtp.gmail.com"
) -> str:
    refresh_token, auth_string = get_oauth_token_and_update_config(
        sender_email, google_client_id, google_client_secret, google_refresh_token
    )
    server = smtplib.SMTP(f'{smtp_server}:{port}')
    server.ehlo(google_client_id)
    server.starttls()
    server.docmd('AUTH', 'XOAUTH2 ' + auth_string)
    server.sendmail(sender_email, receiver_email, message.as_string())
    server.quit()

    return refresh_token

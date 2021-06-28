import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.nonmultipart import MIMENonMultipart
from email.mime.text import MIMEText
from typing import List

from functools import partial

from config.config import CONFIG


class Email(object):
    def __init__(self, smtp_server: str, smtp_port: int, smtp_account: str, smtp_password: str,
                 sender: str, recipients: list):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_account = smtp_account
        self.smtp_password = smtp_password
        self.sender = sender
        self.recipients = recipients
        self.content_list: List[MIMENonMultipart] = []  # [(MIMETYPE,content)....]
        self.mime_map = {
            "text": partial(MIMEText, _subtype="text"),
            "html": partial(MIMEText, _subtype="html"),
            "app": MIMEApplication
        }

    def attach(self, mime="text", *args, **kwargs):
        self.content_list.append(self.mime_map[mime](*args, **kwargs))

    def send(self, title):
        if not self.content_list:
            raise ValueError("缺少内容")
        msg = MIMEMultipart("alternative")
        for content in self.content_list:
            msg.attach(content)

        msg["Subject"] = title
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)

        with smtplib.SMTP(self.smtp_server, self.smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(self.smtp_account, self.smtp_password)
            smtp.send_message(msg)


email = Email(
    CONFIG["ALARM"]["EMAIL"]["SMTP_SERVER"],
    CONFIG["ALARM"]["EMAIL"]["SMTP_PORT"],
    CONFIG["ALARM"]["EMAIL"]["SMTP_ACCOUNT"],
    CONFIG["ALARM"]["EMAIL"]["SMTP_PASSWORD"],
    CONFIG["ALARM"]["EMAIL"]["SENDER"],
    CONFIG["ALARM"]["EMAIL"]["RECIPIENTS"]
)

if __name__ == "__main__":
    email.send("Hello", "Hidsg dsagfdaijf9d0asjcxzbvjad-sgjdapll")

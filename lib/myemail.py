import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from lib.mailgun import Mailgun

class myEmail():
    def __init__(self):
        self.email_from = None
        self.email_to = None
        self.subject = None
        self.body = None
        self.is_configured = False

    def config_email(self, email_from, email_to, subject):
        self.email_from = email_from
        self.email_to = email_to
        self.subject = subject
        self.is_configured = True

    def set_email_subject(self, subject):
        self.subject = subject

    def set_email_from(self, email_from):
        self.email_from = email_from

    def set_email_to(self, email_to):
        self.email_to = email_to

    def send_email_smtp(self, body):
        if not self.is_configured:
            raise Exception("Email setting has not been configured yet!")
        msgRoot = MIMEMultipart('related')
        msgAlternative = MIMEMultipart('alternative')
        msgRoot.attach(msgAlternative)

        msgRoot['Subject'] = self.subject
        msgRoot['From'] = self.email_from
        msgRoot['To'] = self.email_to
        # sendmail() function requires a list but
        # msgRoot has to be comma separated (space is not an issue between comma)
        email_to_list = self.email_to.split(',')
        body_html = MIMEText(body, 'html')
        msgAlternative.attach(body_html)

        s = smtplib.SMTP('localhost')
        s.sendmail(self.email_from, email_to_list, msgRoot.as_string())
        s.quit()
        print("Email is sent to: {m}".format(m=self.email_to))
        print("\n")

    def send_email(self, body):
        if not self.is_configured:
            raise Exception("Email setting has not been configured yet!")
        
        self.mg = Mailgun()
        self.mg.send_mg_email(
            email_from = self.email_from,
            email_to = self.email_to,
            subj = self.subject, 
            html_body = body)
        
        print("Email is sent to: {m}".format(m=self.email_to))
        print("\n")
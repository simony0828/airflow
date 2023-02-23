import os
import requests
import json

# yesterday's date epoch
# date -d "1 day ago 0" +s%
class Mailgun():
    def __init__(self, MAILGUN_API_KEY='MAILGUN_API_KEY'):
        self.url = 'https://api.mailgun.net/'
        self.api_version = 'v3'

        if MAILGUN_API_KEY not in os.environ:
            raise Exception("Missing {v} as environment variable".format(v=MAILGUN_API_KEY))
        self.auth = ("api", os.environ[MAILGUN_API_KEY])
        
        #if run_date not in os.environ:
        #    raise Exception("Missing {v} as environment variable".format(v=run_date))
        #self.run_date=os.environ[run_date]
        #self.headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        #self.parameters = {"start": start_date,
        #                   "end": end_date,
        #                   "resolution": "day",
        #                   "event": ["accepted","delivered","failed","opened","complained"]}

    def send_mg_email(self,email_from, email_to, subj, html_body):
        response = ""
        
        url = "{url}/{api}/upwork.com/messages".format(
            url=self.url,
            api=self.api_version
            )
        
        r = requests.post(url, auth=self.auth,
                          data={"from": email_from,
                                "to": email_to,
                                "subject": subj,
                                "html": html_body})
        
        if not r.ok:
            raise Exception ("Failed to send an email.\n {err}".format(err=r.text))
        else:
            #printing API response to get mail ID for tracking purposes
            response = json.loads(r.text)
            print("Email request has been sent, message id: {id}".format(id=response["id"].strip('<>')))
            

    def __api_call(self, url):
        ''' Internal function to make curl request call '''
        try:
            r = requests.post(url,auth=self.auth, headers=self.headers, params=self.parameters)
        except Exception as e:
            raise e
        return json.loads(r.text)

    def execute(self):
        ''' Function to request data from Mailgun '''
        url = "{url}/{api}/mg.upwork.com/stats/total".format(
            url=self.url,
            api=self.api_version
            )
        response = self.__api_call(url)
        return response
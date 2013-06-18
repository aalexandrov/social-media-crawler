'''
Copyright 2010-2013 DIMA Research Group, TU Berlin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
Created on Jun 18, 2013

@author: Alexander Alexandrov <alexander.alexandrov@tu-berlin.de>
'''

from twython.api import Twython
import webbrowser
import socket
import BaseHTTPServer
import cgi

class OAuthCallbackHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    
    def do_GET(self):
        if self.path.find('?') != -1:
            self.path, self.query_string = self.path.split('?', 1)
        else:
            self.query_string = ''
     
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
     
        self.globals = dict(cgi.parse_qsl(self.query_string))
        
        final_step = self.server.twitter.get_authorized_tokens(self.globals['oauth_verifier'])
        
        self.server.log.info("Handling OAuth HTTP callback.")
        self.server.log.info("Obtained OAuth token is '%s'" % (final_step['oauth_token']))
        self.server.log.info("Obtained OAuth token secret is '%s'" % (final_step['oauth_token_secret']))
        
        print >> self.wfile, "<html>"
        print >> self.wfile, "<head>"
        print >> self.wfile, "  <title>Social Media Crawler &middot; Authentication Service</title>"
        print >> self.wfile, "</head>"
        print >> self.wfile, "<body style='margin: 0; padding: 2ex 2em; font-size: 14px;'>"
        print >> self.wfile, "<div id='header' style='text-align: center;'>"
        print >> self.wfile, "  <h1 style='color: #333; font-size: 2em; margin: 0 0 0.5ex 0; padding: 0;'>Social Media Crawler &middot; Authentication Service</h1>"
        print >> self.wfile, "</div>"
        print >> self.wfile, "<div id='content' style='margin: 1em 2ex;'>"
        print >> self.wfile, "  <p>"
        print >> self.wfile, "    You have authenticated the SMC app with your Twitter account."
        print >> self.wfile, "    Please configure the following parameters in your <em>.crawler-settings</em> file:"
        print >> self.wfile, "  </p>"
        print >> self.wfile, "  <ul>"
        print >> self.wfile, "    <li>TWITTER_OAUTH_TOKEN=%s</li>" % final_step['oauth_token']
        print >> self.wfile, "    <li>TWITTER_OAUTH_TOKEN_SECRET=%s</li>" % final_step['oauth_token_secret']
        print >> self.wfile, "  <ul>"
        print >> self.wfile, "</div>"
        print >> self.wfile, "<body>"
        print >> self.wfile, "</html>"
    
    def log_request(self, code='-', size='-'):
        '''
        Disable request logging for the communication server
        '''
        pass


class OAuthCallbackServer(BaseHTTPServer.HTTPServer):

    log = None
    twitter = None

    def __init__(self, address, log, twitter):
        BaseHTTPServer.HTTPServer.__init__(self, address, OAuthCallbackHandler)
        self.log = log
        self.twitter = twitter

class OAuthWorkflow(object):
    
    _log = None

    def __init__(self, log):
        self._log = log
    
    def run(self, args):
        # configure the server port for the callback
        serverPort = 8000
        
        # initiate OAuth workflow
        twitter = Twython(args.twitter_app_key, args.twitter_app_secret)
        # step 1
        self._log.info("Initiating Twitter OAuth workflow.")
        oAuth = twitter.get_authentication_tokens(callback_url="http://localhost:%d/callback" % (serverPort))
        oAuthToken = oAuth['oauth_token']
        oAuthTokenSecret = oAuth['oauth_token_secret']
        # step 2
        webbrowser.open_new(oAuth['auth_url'])
        # step 3
        self._log.info("Starting OAuth HTTP callback handler.")
        httpd = OAuthCallbackServer(('', serverPort), self._log, Twython(args.twitter_app_key, args.twitter_app_secret, oAuthToken, oAuthTokenSecret))
        httpd.handle_request()

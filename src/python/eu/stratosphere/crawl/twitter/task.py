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

from eu.stratosphere.common import AbstractTask
from eu.stratosphere.error import InvalidSettingsError
from eu.stratosphere.crawl.twitter.auth import OAuthWorkflow
from eu.stratosphere.crawl.twitter.stream import TwitterPrintStreamer

import signal

TASK_PREFIX = "twitter"

class AuthTask(AbstractTask):
    '''
classdocs
'''

    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        kwargs.update(group=TASK_PREFIX, name="auth", description="Authenticate the crawler app for a specific user.")
        super(AuthTask, self).__init__(*args, **kwargs)
    
    def argsParser(self):
        parser = super(AuthTask, self).argsParser()
        
        return parser
        
    def _validateAndFixArgs(self, args):
        super(AuthTask, self)._validateAndFixArgs(args)
        
        requiredSettings = set(['twitter_app_key', 'twitter_app_secret'])
        missingSettings =  requiredSettings - set(filter(None, args.__dict__.keys()))
        if len(missingSettings) > 0:
            raise InvalidSettingsError(self.qname(), list(missingSettings)) 
        
    def _do(self, args):
        self._log.info("Authenticating Twitter user for the SMC.")
        
        workflow = OAuthWorkflow(self._log)
        workflow.run(args)


class CrawlFilterTask(AbstractTask):
    '''
classdocs
'''

    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        kwargs.update(group=TASK_PREFIX, name="filter", description="Crawl a filtered sample of public tweets.")
        super(CrawlFilterTask, self).__init__(*args, **kwargs)
    
    def argsParser(self):
        parser = super(CrawlFilterTask, self).argsParser()
        
        # arguments
        parser.add_argument("--name", metavar="NAME", dest="stream_name", type="str",
                            help="Name of this stream")
        # options
        parser.add_option("-f", "--follow", metavar="UID", dest="follows", action="append", type="str",
                          default=[], help="Crawl a specific user")
        parser.add_option("-l", "--location", metavar="COORDS", dest="locations", action="append", type="str",
                          default=[], help="Crawl a specific location")
        parser.add_option("-k", "--keyword", metavar="WORD", dest="keywords", action="append", type="str",
                          default=[], help="Crawl a specific keyword")
        return parser
        
    def _validateAndFixArgs(self, args):
        super(CrawlFilterTask, self)._validateAndFixArgs(args)
        
        requiredSettings = set(['twitter_app_key', 'twitter_app_secret', 'twitter_oauth_token', 'twitter_oauth_token_secret'])
        missingSettings =  requiredSettings - set(filter(None, args.__dict__.keys()))
        if len(missingSettings) > 0:
            raise InvalidSettingsError(self.qname(), list(missingSettings)) 
        
    def _do(self, args):
        self._log.info("Opening Twitter stream '%s'" % args.stream_name)
        
        streamPath = "%s/%s.stream" % (args.base_path, args.stream_name)
        stream = TwitterPrintStreamer(args.stream_name, 
                                      self._log, 
                                      streamPath, 
                                      args.twitter_app_key, 
                                      args.twitter_app_secret,
                                      args.twitter_oauth_token,
                                      args.twitter_oauth_token_secret)

        def signal_handler(signal, frame):
                stream.disconnect()
                
        signal.signal(signal.SIGINT, signal_handler)
        
        # configure filter params
        params = dict()
        # follows
        if len(args.follows) > 0:
            self._log.info("Configuring stream '%s' to look for users: %s" % (args.stream_name, ', '.join(args.follows)))
            params['follow'] = ', '.join(args.follows)
        # track keywords
        if len(args.keywords) > 0:
            self._log.info("Configuring stream '%s' to look for keywords: %s" % (args.stream_name, ', '.join(args.keywords)))
            params['track'] = ', '.join(args.keywords)
        # locations
        if len(args.locations) > 0:
            self._log.info("Configuring stream '%s' to look for locations: %s" % (args.stream_name, ', '.join(args.locations)))
            params['locations'] = ', '.join(args.locations)

        stream.statuses.filter(**params)

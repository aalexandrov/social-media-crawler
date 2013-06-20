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
from eu.stratosphere.crawl.twitter.auth import OAuthWorkflow
from eu.stratosphere.crawl.twitter.stream import TwitterPrintStreamer

import signal
import bz2
import sets

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
    
    def _requiredSettingsKeys(self):
        keys = AbstractTask._requiredSettingsKeys(self)
        return keys | sets.Set(['twitter_app_key', 'twitter_app_secret'])
        
    def _do(self, args):
        self._log.info("Authenticating Twitter user for the SMC.")
        
        workflow = OAuthWorkflow(self._log)
        workflow.run(args)


class AbstractCrawlTask(AbstractTask):
    '''
    Abstract base class for all tasks that consume a twitter stream.
    '''

    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        super(AbstractCrawlTask, self).__init__(*args, **kwargs)
    
    def argsParser(self):
        parser = super(AbstractCrawlTask, self).argsParser()
        
        # arguments
        parser.add_argument("--name", metavar="NAME", dest="stream_name", type="str",
                            help="Name of this stream")
        # options
        parser.add_option("--compress", metavar="BOOL", dest="compress", action="store_true", 
                              default=False, help="BZip2 the output.")
        parser.add_option("--stall-warnings", metavar="BOOL", dest="stall_warnings", action="store_true",
                              default=False, help="Send stall_warnings messages.")
        parser.add_option("--language", metavar="LANG", dest="languages", action="append", type="str", 
                              default=[], help="Filter only tweets from the given language.")
        
        return parser
    
    def _configureParams(self, args, params):
        '''
        Configure the specific parameters for this stream.
        '''
        # stall_warnings
        if args.stall_warnings:
            self._log.info("Configuring usage of stall-warnings.")
            params['stall_warnings'] = bool(args.stall_warnings)
        # language
        if len(args.languages) > 0:
            self._log.info("Configuring languages %s." % ','.join(args.languages))
            params['languages'] = ','.join(args.languages)
        
        return params
    
    def _requiredSettingsKeys(self):
        keys = AbstractTask._requiredSettingsKeys(self)
        return keys | sets.Set(['twitter_app_key', 'twitter_app_secret', 'twitter_oauth_token', 'twitter_oauth_token_secret'])

    def _do(self, args):
        self._log.info("Opening Twitter stream '%s'" % args.stream_name)
        
        # create print streamer object
        if args.compress:
            wfile = bz2.BZ2File("%s/%s.stream.bz2" % (args.base_path, args.stream_name), "w", 64*1024, 9)
        else:
            wfile = open("%s/%s.stream" % (args.base_path, args.stream_name), "w")
        stream = TwitterPrintStreamer(args.stream_name, 
                                      self._log, 
                                      wfile, 
                                      args.twitter_app_key, 
                                      args.twitter_app_secret,
                                      args.twitter_oauth_token,
                                      args.twitter_oauth_token_secret)

        # register shutdown signal handler
        signal.signal(signal.SIGINT, lambda signal, frame: stream.disconnect())
        
        # configure the stream params and start consuming the stream
        self._startStream(stream, self._configureParams(args, dict()))

    def _startStream(self, stream, params):
        '''
        Start reading from stream. 
        '''
        pass


class CrawlFilterTask(AbstractCrawlTask):
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
        
        # options
        parser.add_option("-f", "--follow", metavar="UID", dest="follows", action="append", type="str",
                          default=[], help="Crawl a specific user.")
        parser.add_option("-l", "--location", metavar="COORD", dest="locations", action="append", type="str",
                          default=[], help="Crawl a specific location.")
        parser.add_option("-k", "--keyword", metavar="WORD", dest="keywords", action="append", type="str",
                          default=[], help="Crawl a specific keyword.")
        return parser

    def _configureParams(self, args, params):
        params = AbstractCrawlTask._configureParams(self, args, params)
        
        # follows
        if len(args.follows) > 0:
            self._log.info("Configuring stream '%s' to look for users: %s" % (args.stream_name, ', '.join(args.follows)))
            params['follow'] = ', '.join(args.follows)
        # locations
        if len(args.locations) > 0:
            self._log.info("Configuring stream '%s' to look for locations: %s" % (args.stream_name, ', '.join(args.locations)))
            params['locations'] = ', '.join(args.locations)
        # track keywords
        if len(args.keywords) > 0:
            self._log.info("Configuring stream '%s' to look for keywords: %s" % (args.stream_name, ', '.join(args.keywords)))
            params['track'] = ', '.join(args.keywords)

        # return configured params
        return params
    
    def _requiredSettingsKeys(self):
        keys = AbstractTask._requiredSettingsKeys(self)
        return keys | sets.Set(['twitter_app_key', 'twitter_app_secret', 'twitter_oauth_token', 'twitter_oauth_token_secret'])
        
    def _startStream(self, stream, params):
        stream.statuses.filter(**params)


class CrawlSampleTask(AbstractCrawlTask):
    '''
classdocs
'''

    def __init__(self, *args, **kwargs):
        '''
        Constructor
        '''
        kwargs.update(group=TASK_PREFIX, name="sample", description="Crawl a sample of all public tweets.")
        super(CrawlSampleTask, self).__init__(*args, **kwargs)
    
    def _startStream(self, stream, params):
        stream.statuses.sample(**params)

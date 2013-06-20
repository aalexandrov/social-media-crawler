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
from twython.streaming.api import TwythonStreamer
import json
import datetime

class TwitterPrintStreamer(TwythonStreamer):
    
    _name = None
    _log = None
    _wfile = None
    _currEpochStart = None
    _count = 0
    
    EPOCH_SIZE = 1000
    
    def __init__(self, name, log, wfile, app_key, app_secret, oauth_token, oauth_token_secret, timeout=300, retry_count=None, retry_in=10, headers=None):
        super(TwitterPrintStreamer, self).__init__(app_key, app_secret, oauth_token, oauth_token_secret, timeout, retry_count, retry_in, headers)
        self._name = name
        self._log = log
        self._wfile = wfile
        self._currEpochStart = datetime.datetime.now().replace(microsecond=0)
        self._count = 0
    
    def on_success(self, data):
        print >> self._wfile, json.dumps(data, sort_keys=True, check_circular=False, indent=4, separators=(',', ': ')) + ","
        self._count = self._count + 1
        if (self._count % TwitterPrintStreamer.EPOCH_SIZE == 0):
            oldEpochStart = self._currEpochStart 
            self._currEpochStart = datetime.datetime.now().replace(microsecond=0)
            self._log.info("Collected next batch of %d tweets in %s seconds." % (TwitterPrintStreamer.EPOCH_SIZE, self._currEpochStart - oldEpochStart))

    def on_timeout(self):
        self._log.warn("Timeout in twitter stream %s." % (self._name))

    def on_limit(self, data):
        self._log.warn("Limit reached for twitter stream %s: data." % (self._name, data))

    def on_error(self, status_code, data):
        self._log.error("Error in twitter stream %s: %s." % (self._name, data))

    def disconnect(self):
        self._log.info("Disconnecting twitter stream.")
        TwythonStreamer.disconnect(self)

        if self._wfile is not None: 
            self._wfile.close()

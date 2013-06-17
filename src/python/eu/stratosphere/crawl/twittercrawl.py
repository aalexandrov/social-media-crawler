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
Created on Oct 14, 2011

@author: Alexander Alexandrov <alexander.alexandrov@tu-berlin.de>
'''

from subprocess import Popen, PIPE
from re import sub
from eu.stratosphere.crawl.common import AbstractTask

import twitter

TASK_PREFIX = "twitter"

class CrawlTask(AbstractTask):
    '''
classdocs
'''

    def __init__(self, *args, **kwargs):
        '''
Constructor
'''
        kwargs.update(group=TASK_PREFIX, name="statuses", description="Crawl a random sample of all public Twitter statuses.")

        super(CrawlTask, self).__init__(*args, **kwargs)
    
    def argsParser(self):
        parser = super(CrawlTask, self).argsParser()
        
        # arguments
#         parser.add_option("--prototype-file", metavar="PROTOTYPE", dest="prototype_path", type="str",
#                           default=None, help="path to the compiled XML prototype file (defaults to `${config-dir}/${dgen-name}-prototype.xml`)")
        return parser
        
    def _fixArgs(self, args):
        super(CrawlTask, self)._fixArgs(args)
        
    def _do(self, args):
        self._log.info("Crawling tweets...")
        api = twitter.Api()
        
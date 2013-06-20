# Social Media Crawler

The *Social Media Crawler* (short *SMC*) is a small python library that can be used to collect streaming data from various social API endpoints such as Twtiter, Facebook, Tumblr, etc.

## Examples

The *SMC* is based around the notion of configurable command line tasks. Each social media endpoint API can be accessed and crawled in a focused or unfocused way using the appropriate set of tasks.

Here are a few examples:

```
# grab a public sample of all tweets in Bulgarian language
bin/crawler.py twitter:sample --language=BG bulgaria.sample

# filter everything within the bounding box of Bulgaria
bin/crawler.py twitter:filter -l 22.37,41,28.6,44.19 --language=BG bulgaria.domestic

# filter tweets about the Arsenal Football Club based on keywords
bin/crawler.py twitter:filter -k AFC -k Arsenal -k Wenger arsenal
```

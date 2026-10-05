# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Offline checks for recent-news routing, linked RSS evidence, and fallback."""
from datetime import datetime, timezone

import httpx
import pytest

from arena.runtime import _news_query, web_search


NEWS_RSS = '''<?xml version="1.0"?><rss version="2.0"><channel><item>
  <title>Bitcoin market update - Example Finance</title>
  <link>https://news.google.com/rss/articles/fixture-article</link>
  <pubDate>Sun, 04 Oct 2026 08:15:00 GMT</pubDate>
  <description>&lt;a href="https://news.google.com/rss/articles/fixture-article"&gt;Bitcoin market update&lt;/a&gt; &lt;font&gt;Example Finance&lt;/font&gt;</description>
  <source url="https://finance.example.com">Example Finance</source>
</item><item><title>Private link must be excluded</title>
  <link>http://127.0.0.1/private</link><description>Unsafe result.</description>
</item></channel></rss>'''

BING_RSS = '''<rss><channel><item><title>Bitcoin reference</title>
  <link>https://example.com/bitcoin</link><description>A supplied RSS snippet.</description>
</item></channel></rss>'''


async def test_news_search_preserves_link_published_time_snippet_and_publisher():
    requested = []

    def handle(request):
        requested.append(request.url)
        assert request.url.host == 'news.google.com'
        assert request.url.path == '/rss/search'
        assert request.url.params['q'] == 'Bitcoin Ethereum latest news when:3d'
        assert request.url.params['hl'] == 'en-US'
        assert request.url.params['gl'] == 'US'
        assert request.url.params['ceid'] == 'US:en'
        assert 'authorization' not in request.headers
        return httpx.Response(200, text=NEWS_RSS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        found = await web_search('Bitcoin Ethereum latest news', client=client, api_url='')
    assert len(requested) == 1
    assert found['source'] == 'Google News RSS'
    assert found['query'] == 'Bitcoin Ethereum latest news'
    assert found['searchQuery'] == 'Bitcoin Ethereum latest news when:3d'
    assert found['results'] == [{
        'title': 'Bitcoin market update - Example Finance',
        'url': 'https://news.google.com/rss/articles/fixture-article',
        'snippet': 'Bitcoin market update Example Finance',
        'publishedAt': 'Sun, 04 Oct 2026 08:15:00 GMT',
        'publisher': 'Example Finance', 'publisherUrl': 'https://finance.example.com',
    }]


async def test_general_web_query_uses_bing_directly():
    requested = []

    def handle(request):
        requested.append(request.url.host)
        assert request.url.host == 'www.bing.com'
        assert request.url.params['q'] == 'Bitcoin total supply'
        return httpx.Response(200, text=BING_RSS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        found = await web_search('Bitcoin total supply', client=client, api_url='')
    assert requested == ['www.bing.com'] and found['source'] == 'Bing RSS'


@pytest.mark.parametrize('response', [httpx.Response(503), httpx.Response(200, text='<rss><channel/></rss>'),
                                      httpx.Response(200, text='not valid XML')])
async def test_news_empty_error_or_bad_feed_falls_back_to_bing(response):
    requested = []

    def handle(request):
        requested.append(request.url.host)
        if request.url.host == 'news.google.com':
            return response
        assert request.url.host == 'www.bing.com'
        return httpx.Response(200, text=BING_RSS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        found = await web_search('Ethereum ETF flows news', client=client, api_url='')
    assert requested == ['news.google.com', 'www.bing.com']
    assert found['source'] == 'Bing RSS' and found['results'][0]['url'] == 'https://example.com/bitcoin'


async def test_configured_endpoint_has_priority_and_search_key_stays_there():
    requested = []

    def handle(request):
        requested.append(request.url.host)
        if request.url.host == 'search.example.com':
            assert request.headers['authorization'] == 'Bearer fixture-search-secret'
            return httpx.Response(503)
        assert request.url.host == 'news.google.com'
        assert 'authorization' not in request.headers
        return httpx.Response(200, text=NEWS_RSS)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        found = await web_search('Ethereum latest news', client=client,
                                 api_url='https://search.example.com/search', api_key='fixture-search-secret')
    assert requested == ['search.example.com', 'news.google.com']
    assert found['source'] == 'Google News RSS'


@pytest.mark.parametrize('query', ['Bitcoin announcements', 'Ethereum event', 'ETH ETF flow',
                                   'Apple earnings', '比特币最新消息'])
def test_news_keywords_are_routed_to_recent_news(query):
    assert _news_query(query) == query + ' when:3d'


def test_explicit_dates_windows_and_current_date_are_respected(monkeypatch):
    monkeypatch.setattr('arena.runtime._now', lambda: datetime(2026, 10, 4, 8, tzinfo=timezone.utc))
    assert _news_query('Bitcoin latest news 2026-10-04') == 'Bitcoin latest news when:3d'
    assert _news_query('Bitcoin news when:1d') == 'Bitcoin news when:1d'
    assert _news_query('Bitcoin news after:2026-09-01') == 'Bitcoin news after:2026-09-01'
    assert _news_query('Bitcoin news after:2026-10-04') == 'Bitcoin news after:2026-10-04'
    assert _news_query('Bitcoin news 2021-05-01') == 'Bitcoin news 2021-05-01'
    assert _news_query('Bitcoin total supply') is None

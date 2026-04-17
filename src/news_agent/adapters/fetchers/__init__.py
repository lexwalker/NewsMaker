"""Fetcher adapters: RSS, HTML, Playwright."""

from news_agent.adapters.fetchers.base import Fetcher, RateLimiter, RobotsCache
from news_agent.adapters.fetchers.html import HTMLFetcher
from news_agent.adapters.fetchers.rss import RSSFetcher

__all__ = ["Fetcher", "HTMLFetcher", "RSSFetcher", "RateLimiter", "RobotsCache"]

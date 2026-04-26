RSS_SOURCES = [
    {
        "source_name": "EIA Today in Energy",
        "source_type": "rss",
        "base_url": "https://www.eia.gov",
        "feed_url": "https://www.eia.gov/rss/todayinenergy.xml",
        "domain_name": "eia.gov",
        "topic_focus": "energy,oil_market,supply,demand",
    },
    {
        "source_name": "EIA Press Releases",
        "source_type": "rss",
        "base_url": "https://www.eia.gov",
        "feed_url": "https://www.eia.gov/rss/press_rss.xml",
        "domain_name": "eia.gov",
        "topic_focus": "energy,oil_market,supply,demand",
    },
    {
        "source_name": "ECB Press Releases",
        "source_type": "rss",
        "base_url": "https://www.ecb.europa.eu",
        "feed_url": "https://www.ecb.europa.eu/rss/press.html",
        "domain_name": "ecb.europa.eu",
        "topic_focus": "economy,tariffs,sanctions,macro",
    },
    {
        "source_name": "ECB Blog",
        "source_type": "rss",
        "base_url": "https://www.ecb.europa.eu",
        "feed_url": "https://www.ecb.europa.eu/rss/blog.html",
        "domain_name": "ecb.europa.eu",
        "topic_focus": "economy,macro,policy",
    },
    {
        "source_name": "IMF News",
        "source_type": "rss",
        "base_url": "https://www.imf.org",
        "feed_url": "https://www.imf.org/en/news/rss",
        "domain_name": "imf.org",
        "topic_focus": "economy,macro,sanctions,tariffs,geopolitics",
    },
    {
        "source_name": "Federal Reserve All Press Releases",
        "source_type": "rss",
        "base_url": "https://www.federalreserve.gov",
        "feed_url": "https://www.federalreserve.gov/feeds/press_all.xml",
        "domain_name": "federalreserve.gov",
        "topic_focus": "economy,macro,policy,interest_rates,financial_markets",
    },
    {
        "source_name": "Federal Reserve Monetary Policy",
        "source_type": "rss",
        "base_url": "https://www.federalreserve.gov",
        "feed_url": "https://www.federalreserve.gov/feeds/press_monetary.xml",
        "domain_name": "federalreserve.gov",
        "topic_focus": "economy,macro,policy,interest_rates,financial_markets",
    },
    {
        "source_name": "Federal Reserve Balance Sheet",
        "source_type": "rss",
        "base_url": "https://www.federalreserve.gov",
        "feed_url": "https://www.federalreserve.gov/feeds/clp.xml",
        "domain_name": "federalreserve.gov",
        "topic_focus": "economy,macro,liquidity,financial_markets,banking",
    },
    {
        "source_name": "EU Sanctions Guidance",
        "source_type": "rss",
        "base_url": "https://finance.ec.europa.eu",
        "feed_url": "https://finance.ec.europa.eu/node/1296/rss_en",
        "domain_name": "finance.ec.europa.eu",
        "topic_focus": "sanctions,tariffs,trade,geopolitics,energy",
    },
    {
        "source_name": "EU Sanctions FAQs",
        "source_type": "rss",
        "base_url": "https://finance.ec.europa.eu",
        "feed_url": "https://finance.ec.europa.eu/node/1068/rss_en",
        "domain_name": "finance.ec.europa.eu",
        "topic_focus": "sanctions,trade,geopolitics,energy,finance",
    },
    {
        "source_name": "EU Sanctions Energy FAQs",
        "source_type": "rss",
        "base_url": "https://finance.ec.europa.eu",
        "feed_url": "https://finance.ec.europa.eu/node/1068/rss_en?f%5B0%5D=sanctions_category_sanctions_category%3A154",
        "domain_name": "finance.ec.europa.eu",
        "topic_focus": "sanctions,energy,oil_market,geopolitics,trade",
    },
]


API_SOURCES = [
    {
        "source_name": "Hacker News Oil Search",
        "source_type": "api",
        "base_url": "https://hn.algolia.com",
        "feed_url": (
            "https://hn.algolia.com/api/v1/search_by_date"
            "?query=oil%20OR%20opec%20OR%20brent%20OR%20wti"
            "&tags=story"
            "&hitsPerPage=50"
        ),
        "domain_name": "hn.algolia.com",
        "topic_focus": "social,technology,investor_discussion,oil_market",
    },
    {
        "source_name": "Hacker News Energy Search",
        "source_type": "api",
        "base_url": "https://hn.algolia.com",
        "feed_url": (
            "https://hn.algolia.com/api/v1/search_by_date"
            "?query=energy%20prices%20OR%20oil%20market%20OR%20crude"
            "&tags=story"
            "&hitsPerPage=50"
        ),
        "domain_name": "hn.algolia.com",
        "topic_focus": "social,technology,energy,oil_market",
    },
    {
        "source_name": "Mastodon Oil Hashtag",
        "source_type": "api",
        "base_url": "https://mastodon.social",
        "feed_url": "https://mastodon.social/api/v1/timelines/tag/oil?limit=40",
        "domain_name": "mastodon.social",
        "topic_focus": "social,oil_market,public_posts",
    },
    {
        "source_name": "Mastodon Energy Hashtag",
        "source_type": "api",
        "base_url": "https://mastodon.social",
        "feed_url": "https://mastodon.social/api/v1/timelines/tag/energy?limit=40",
        "domain_name": "mastodon.social",
        "topic_focus": "social,energy,public_posts",
    },
    {
        "source_name": "Mastodon OPEC Hashtag",
        "source_type": "api",
        "base_url": "https://mastodon.social",
        "feed_url": "https://mastodon.social/api/v1/timelines/tag/opec?limit=40",
        "domain_name": "mastodon.social",
        "topic_focus": "social,opec,oil_market,public_posts",
    },
]


ALL_SOURCES = RSS_SOURCES + API_SOURCES
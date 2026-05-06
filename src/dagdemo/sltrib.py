import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from html import unescape

import requests
from dagster import (
    asset,
    AssetExecutionContext,
    AssetIn,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
    Jitter,
)

SLTRIB_RSS_URL = "https://www.sltrib.com/arc/outboundfeeds/news/?outputType=xml"

DEFAULT_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=30,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

IPAD_USER_AGENT = (
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.0 Mobile/15E148 Safari/604.1"
)

DEFAULT_HEADERS = {
    "User-Agent": IPAD_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class ArticleImage:
    url: str = ""
    alt_text: str = ""
    is_thumbnail: bool = False


@dataclass
class Article:
    source: str = "unknown"
    title: str = ""
    link: str = ""
    pub_date: datetime | None = None
    author: str | None = None
    description: str = ""
    content: str = ""
    full_content: str = ""
    images: list[ArticleImage] = field(default_factory=list)
    thumbnail_url: str | None = None
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    content_fetched_at: datetime | None = None

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "title": self.title,
            "link": self.link,
            "pub_date": self.pub_date.isoformat() if self.pub_date else None,
            "author": self.author,
            "description": self.description,
            "content": self.content,
            "full_content": self.full_content,
            "thumbnail_url": self.thumbnail_url,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Article":
        pub_date = None
        if data.get("pub_date"):
            try:
                pub_date = datetime.fromisoformat(data["pub_date"])
            except (ValueError, TypeError):
                pass
        return cls(
            source=data.get("source", "unknown"),
            title=data.get("title", ""),
            link=data.get("link", ""),
            pub_date=pub_date,
            author=data.get("author"),
            description=data.get("description", ""),
            content=data.get("content", ""),
            full_content=data.get("full_content", ""),
            thumbnail_url=data.get("thumbnail_url"),
        )


def strip_html_tags(text: str) -> str:
    clean = re.sub(r"<[^>]+>", "", text)
    return unescape(clean).strip()


def parse_pub_date(date_str: str) -> datetime | None:
    if not date_str:
        return None
    formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def is_image_url(url: str) -> bool:
    if not url:
        return False
    lower = url.lower()
    image_extensions = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp")
    if any(lower.endswith(ext) or ext + "?" in lower for ext in image_extensions):
        return True
    if any(pattern in lower for pattern in ["image", "img", "photo", "media", "cdn"]):
        return True
    return False


def parse_sltrib_item(item: ET.Element) -> dict:
    def get_text(tag: str) -> str:
        elem = item.find(tag)
        return elem.text if elem is not None and elem.text else ""

    namespaces = {
        "dc": "http://purl.org/dc/elements/1.1/",
        "content": "http://purl.org/rss/1.0/modules/content/",
    }

    creator_elem = item.find("dc:creator", namespaces)
    creator = (
        creator_elem.text if creator_elem is not None and creator_elem.text else ""
    )

    content_elem = item.find("content:encoded", namespaces)
    content = (
        content_elem.text
        if content_elem is not None and content_elem.text
        else get_text("description")
    )

    return {
        "title": get_text("title"),
        "link": get_text("link"),
        "pub_date": get_text("pubDate"),
        "author": creator,
        "creator": creator,
        "description": strip_html_tags(get_text("description")),
        "content": content,
    }


def extract_images_from_rss_item(item: ET.Element) -> tuple[list[ArticleImage], str | None]:
    images: list[ArticleImage] = []
    thumbnail_url: str | None = None

    for media in item.findall("media:content", {"media": "http://search.yahoo.com/mrss/"}):
        url = media.get("url")
        if url and is_image_url(url):
            images.append(ArticleImage(url=url))
            if thumbnail_url is None:
                thumbnail_url = url

    for thumb in item.findall("media:thumbnail", {"media": "http://search.yahoo.com/mrss/"}):
        url = thumb.get("url")
        if url:
            images.append(ArticleImage(url=url, is_thumbnail=True))
            thumbnail_url = url

    for tag in ["description", "content:encoded"]:
        ns = {"content": "http://purl.org/rss/1.0/modules/content/"} if ":" in tag else {}
        elem = item.find(tag, ns)
        if elem is not None and elem.text:
            img_matches = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', elem.text)
            for img_url in img_matches:
                if is_image_url(img_url) and not any(i.url == img_url for i in images):
                    images.append(ArticleImage(url=img_url))
                    if thumbnail_url is None:
                        thumbnail_url = img_url

    return images, thumbnail_url


def fetch_article_content(url: str, context=None) -> tuple[str, list[ArticleImage]]:
    try:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        response = session.get(url, timeout=30)
        response.raise_for_status()
        html = response.text

        content = ""
        for pattern in [
            r"<article[^>]*>(.*?)</article>",
            r'<div[^>]*class="[^"]*article-body[^"]*"[^>]*>(.*?)</div>',
            r'<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>',
            r"<main[^>]*>(.*?)</main>",
        ]:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                content = match.group(1)
                break

        if not content:
            body_match = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
            if body_match:
                content = body_match.group(1)

        images: list[ArticleImage] = []
        img_matches = re.findall(
            r'<img[^>]+src=["\']([^"\']+)["\'][^>]*(?:alt=["\']([^"\']*)["\'])?',
            content or html,
        )
        for img_url, alt_text in img_matches:
            absolute_url = requests.compat.urljoin(url, img_url)
            if is_image_url(absolute_url):
                images.append(ArticleImage(url=absolute_url, alt_text=alt_text or ""))

        if content:
            content = re.sub(r"<script[^>]*>.*?</script>", "", content, flags=re.DOTALL | re.IGNORECASE)
            content = re.sub(r"<style[^>]*>.*?</style>", "", content, flags=re.DOTALL | re.IGNORECASE)
            content = re.sub(r"<nav[^>]*>.*?</nav>", "", content, flags=re.DOTALL | re.IGNORECASE)
            content = re.sub(r"<br\s*/?>", "\n", content)
            content = re.sub(r"</p>", "\n\n", content)
            content = re.sub(r"<p[^>]*>", "", content)
            content = re.sub(r"</?(h[1-6])[^>]*>", "\n\n", content)
            content = strip_html_tags(content)
            content = re.sub(r"\n{3,}", "\n\n", content)
            content = content.strip()

        return content, images

    except (requests.RequestException, ValueError, AttributeError) as e:
        if context:
            context.log.warning(f"Failed to fetch article content from {url}: {e}")
        return "", []


@asset(
    name="sltrib_feed_xml",
    group_name="sltrib",
    description="Fetches the Salt Lake Tribune RSS feed.",
    retry_policy=DEFAULT_RETRY_POLICY,
)
def sltrib_feed_xml(context: AssetExecutionContext) -> str:
    context.log.info(f"Downloading feed from {SLTRIB_RSS_URL}")
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    response = session.get(SLTRIB_RSS_URL, timeout=30)
    response.raise_for_status()
    context.log.info(f"Downloaded {len(response.text)} bytes")
    return response.text


@asset(
    name="sltrib_articles",
    group_name="sltrib",
    description="Parses the SLTrib RSS XML into Article objects.",
    ins={"feed_xml_input": AssetIn(key="sltrib_feed_xml")},
)
def sltrib_articles(context: AssetExecutionContext, feed_xml_input: str) -> MaterializeResult:
    try:
        root = ET.fromstring(feed_xml_input)
    except ET.ParseError as e:
        context.log.error(f"Failed to parse RSS XML: {e}")
        return MaterializeResult(value=[], metadata={"error": f"XML parse error: {e}"})

    channel = root.find("channel")
    if channel is None:
        context.log.warning("No channel found in RSS feed")
        return MaterializeResult(value=[], metadata={"error": "No channel in feed"})

    parsed_articles: list[Article] = []
    total_images = 0

    for item in channel.findall("item"):
        raw = parse_sltrib_item(item)
        images, thumbnail_url = extract_images_from_rss_item(item)
        total_images += len(images)

        article = Article(
            source="sltrib",
            title=raw.get("title", ""),
            link=raw.get("link", ""),
            pub_date=parse_pub_date(raw.get("pub_date", "")),
            author=raw.get("author") or raw.get("creator"),
            description=raw.get("description", ""),
            content=raw.get("content", ""),
            images=images,
            thumbnail_url=thumbnail_url,
        )
        parsed_articles.append(article)

    context.log.info(f"Parsed {len(parsed_articles)} articles with {total_images} images")

    metadata: dict = {
        "article_count": len(parsed_articles),
        "total_images_in_feed": total_images,
    }

    if parsed_articles:
        titles_md = "\n".join(f"- {a.title}" for a in parsed_articles[:10])
        if len(parsed_articles) > 10:
            titles_md += f"\n- ... and {len(parsed_articles) - 10} more"
        metadata["titles"] = MetadataValue.md(titles_md)

        first = parsed_articles[0]
        preview_lines = [
            f"## {first.title}",
            "",
            f"**Source:** {first.source}",
            f"**Author:** {first.author or 'Unknown'}",
            f"**Published:** {first.pub_date.strftime('%Y-%m-%d %H:%M') if first.pub_date else 'Unknown'}",
            f"**Link:** [{first.link}]({first.link})",
            "",
            "---",
            "",
            f"> {first.description[:500]}..." if len(first.description) > 500 else f"> {first.description}",
        ]
        metadata["preview"] = MetadataValue.md("\n".join(preview_lines))

        if first.thumbnail_url:
            metadata["first_thumbnail"] = MetadataValue.url(first.thumbnail_url)

    articles_data = [a.to_dict() for a in parsed_articles]
    return MaterializeResult(value=articles_data, metadata=metadata)


@asset(
    name="sltrib_full_content",
    group_name="sltrib",
    description="Fetches full article content from URLs.",
    ins={"articles_input": AssetIn(key="sltrib_articles")},
    retry_policy=DEFAULT_RETRY_POLICY,
)
def sltrib_full_content(
    context: AssetExecutionContext,
    articles_input: list[dict],
) -> MaterializeResult:
    enriched_articles: list[Article] = []
    total_content_fetched = 0
    failed_fetches = 0

    for article_dict in articles_input:
        article = Article.from_dict(article_dict)

        if article.link:
            context.log.info(f"Fetching content from: {article.link}")
            full_text, page_images = fetch_article_content(article.link, context)

            if full_text:
                article.full_content = full_text
                article.content_fetched_at = datetime.now(timezone.utc)
                total_content_fetched += 1

                existing_urls = {img.url for img in article.images}
                for img in page_images:
                    if img.url not in existing_urls:
                        article.images.append(img)
            else:
                failed_fetches += 1

        enriched_articles.append(article)

    context.log.info(
        f"Enriched {len(enriched_articles)} articles: "
        f"{total_content_fetched} content fetched, {failed_fetches} failed"
    )

    metadata = {
        "article_count": len(enriched_articles),
        "content_fetched": total_content_fetched,
        "failed_fetches": failed_fetches,
    }

    if enriched_articles:
        first = enriched_articles[0]
        preview_lines = [
            f"## {first.title}",
            "",
            f"**Full content length:** {len(first.full_content)} chars",
        ]
        if first.full_content:
            preview_lines.extend([
                "**Content preview:**",
                f"> {first.full_content[:500]}...",
            ])
        metadata["preview"] = MetadataValue.md("\n".join(preview_lines))

    articles_data = [a.to_dict() for a in enriched_articles]
    return MaterializeResult(value=articles_data, metadata=metadata)


sltrib_assets = [sltrib_feed_xml, sltrib_articles, sltrib_full_content]

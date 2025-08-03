"""
Documentation search module for semantic search over Materialize documentation.
Uses S3-based vector storage for sharing across MCP servers.
"""

import logging
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from .s3_vector_bucket_store import get_s3_vector_bucket_store


logger = logging.getLogger(__name__)


class DocumentationSearcher:
    """Handles semantic search over Materialize documentation using S3 Vector Buckets."""
    
    def __init__(
        self, 
        bucket_name: str = "materialize-docs-vectors",
        region: str = "us-east-1"
    ):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_store = None
        self.base_url = "https://materialize.com/docs/"
        
    async def initialize(self):
        """Initialize the searcher with S3 Vector Bucket store."""
        logger.info("Initializing documentation searcher...")
        
        # Initialize S3 Vector Bucket store
        self.s3_store = await get_s3_vector_bucket_store(self.bucket_name, self.region)
        logger.info("S3 Vector Bucket store initialized")
    
    async def scrape_documentation(self, max_pages: int = 200):
        """Scrape Materialize documentation and store in S3 Vector Buckets."""
        if not self.s3_store:
            await self.initialize()
            
        logger.info(f"Starting documentation scrape (max {max_pages} pages)")
        
        scraped_docs = []
        async for doc_batch in self._scrape_documentation_generator(max_pages):
            scraped_docs.extend(doc_batch)
        
        # Bulk add all documents to S3 Vector Buckets
        if scraped_docs:
            await self.s3_store.bulk_add_documents(scraped_docs)
                    
        logger.info(f"Scraping completed. Processed {len(scraped_docs)} pages")
    
    async def _scrape_documentation_generator(self, max_pages: int = 200, batch_size: int = 10):
        """Generator that yields batches of scraped documents."""
        logger.info(f"Starting documentation scrape (max {max_pages} pages)")
        
        visited_urls = set()
        to_visit = [self.base_url]
        scraped_docs = []
        total_scraped = 0
        
        async with aiohttp.ClientSession() as session:
            while to_visit and total_scraped < max_pages:
                url = to_visit.pop(0)
                if url in visited_urls:
                    continue
                    
                visited_urls.add(url)
                
                try:
                    logger.debug(f"Scraping: {url}")
                    doc_data = await self._scrape_page(session, url)
                    
                    if doc_data:
                        scraped_docs.append(doc_data)
                        total_scraped += 1
                        
                        # Find more links to scrape
                        new_links = await self._extract_doc_links(session, url)
                        for link in new_links:
                            if link not in visited_urls and link not in to_visit:
                                to_visit.append(link)
                        
                        # Yield batch when we have enough documents
                        if len(scraped_docs) >= batch_size:
                            yield scraped_docs
                            scraped_docs = []
                                
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    continue
        
        # Yield remaining documents
        if scraped_docs:
            yield scraped_docs
        
    async def _scrape_page(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
        """Scrape a single documentation page."""
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Extract title
                title_elem = soup.find('h1') or soup.find('title')
                title = title_elem.get_text().strip() if title_elem else "Untitled"
                
                # Extract main content
                content_elem = (
                    soup.find('main') or 
                    soup.find('article') or 
                    soup.find('div', class_='content') or
                    soup.find('div', class_='documentation')
                )
                
                if not content_elem:
                    content_elem = soup.find('body')
                    
                if not content_elem:
                    return None
                
                # Clean up content
                content = self._extract_text_content(content_elem)
                
                if not content.strip():
                    return None
                    
                # Extract section information from URL or breadcrumbs
                section, subsection = self._extract_section_info(soup, url)
                
                return {
                    'url': url,
                    'title': title,
                    'content': content,
                    'section': section,
                    'subsection': subsection
                }
                
        except Exception as e:
            logger.error(f"Error scraping page {url}: {e}")
            return None
    
    def _extract_text_content(self, element) -> str:
        """Extract clean text content from HTML element."""
        # Remove script and style elements
        for script in element(["script", "style", "nav", "footer", "header"]):
            script.decompose()
            
        # Get text and clean it up
        text = element.get_text()
        
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _extract_section_info(self, soup: BeautifulSoup, url: str) -> tuple[str, str]:
        """Extract section and subsection information."""
        # Try to get from breadcrumbs
        breadcrumb = soup.find('nav', class_='breadcrumb') or soup.find('ol', class_='breadcrumb')
        if breadcrumb:
            links = breadcrumb.find_all('a')
            if len(links) >= 2:
                section = links[1].get_text().strip()
                subsection = links[-1].get_text().strip() if len(links) > 2 else ""
                return section, subsection
        
        # Fallback to URL parsing
        path_parts = urlparse(url).path.strip('/').split('/')
        if len(path_parts) >= 2:
            section = path_parts[1].replace('-', ' ').title()
            subsection = path_parts[2].replace('-', ' ').title() if len(path_parts) > 2 else ""
            return section, subsection
            
        return "General", ""
    
    async def _extract_doc_links(self, session: aiohttp.ClientSession, url: str) -> List[str]:
        """Extract documentation links from a page."""
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return []
                    
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                links = []
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    full_url = urljoin(url, href)
                    
                    # Only include documentation links
                    if (full_url.startswith(self.base_url) and 
                        not any(skip in full_url for skip in ['#', 'mailto:', 'tel:', '.pdf', '.zip'])):
                        links.append(full_url)
                        
                return links
                
        except Exception as e:
            logger.error(f"Error extracting links from {url}: {e}")
            return []
    
    
    async def search(self, query: str, limit: int = 5, section_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Perform semantic search over the documentation."""
        if not self.s3_store:
            await self.initialize()
        
        return await self.s3_store.search(query, limit, section_filter)
    
    async def get_document_count(self) -> int:
        """Get the total number of documents in the S3 store."""
        if not self.s3_store:
            await self.initialize()
        
        return await self.s3_store.get_document_count()


# Global instance
_searcher = None

async def get_searcher(
    bucket_name: str = "materialize-docs-vectors",
    region: str = "us-east-1"
) -> DocumentationSearcher:
    """Get or create the global documentation searcher instance."""
    global _searcher
    if _searcher is None:
        _searcher = DocumentationSearcher(bucket_name=bucket_name, region=region)
        await _searcher.initialize()
    return _searcher
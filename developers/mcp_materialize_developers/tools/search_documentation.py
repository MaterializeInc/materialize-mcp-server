from typing import Optional

from ..search.doc_search import DocumentationSearcher


class SearchDocumentation:
    __name__ = "search_documentation"

    def __init__(self):
        self.searcher = DocumentationSearcher()

    async def __call__(
        self, query: str, limit: int = 5, section_filter: Optional[str] = None
    ):
        """
        Search Materialize documentation using semantic search to find relevant information and answers.
        Always search the documentation before writing SQL as details of how Materialize works may have changed.

        Args:
            query: The search query or question to find relevant documentation
            limit: The maximum number of documents to return (default 5)
            section_filter: Optional section filter to limit search to specific sections
        """

        return await self.searcher.search(query, limit, section_filter)

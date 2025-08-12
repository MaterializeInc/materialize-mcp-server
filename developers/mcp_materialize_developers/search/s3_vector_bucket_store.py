"""
S3 Vector Buckets-based vector store for documentation search.
Uses AWS S3 Vector Buckets native vector storage with local embeddings.
"""

import asyncio
import logging
from typing import List, Dict, Any, Optional
import concurrent.futures

import boto3
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class S3VectorBucketStore:
    """S3 Vector Buckets-based vector store for documentation embeddings."""

    def __init__(
        self,
        bucket_name: str = "materialize-docs-vectors",
        region: str = "us-east-1",
        index_name: str = "docs-index",
        embedding_model: str = "all-MiniLM-L6-v2",
    ):
        """
        Initialize S3 Vector Bucket store (synchronously).

        Args:
            bucket_name: Name of the S3 vector bucket
            region: AWS region (must support S3 Vectors preview)
            index_name: Name of the vector index within the bucket
            embedding_model: Local embedding model name
        """
        logger.info("Initializing S3 Vector Bucket store...")

        self.bucket_name = bucket_name
        self.region = region
        self.index_name = index_name
        self.embedding_model = embedding_model

        # These will be set below
        self.s3vectors_client: Optional[object] = None
        self.local_model: Optional[SentenceTransformer] = None
        self.vector_dimensions: Optional[int] = None

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
        # Initialize S3 Vectors client
        self.s3vectors_client = boto3.client("s3vectors", region_name=self.region)

        self.local_model = SentenceTransformer(self.embedding_model)
        test_embedding = self.local_model.encode("test")
        self.vector_dimensions = len(test_embedding)
        logger.info(
            "Using local model '%s' with %d dimensions",
            self.embedding_model,
            self.vector_dimensions,
        )

        self._ensure_bucket_and_index()
        logger.info("S3 Vector Bucket store initialized")

    async def _get_embedding(self, text: str) -> List[float]:
        """Get embedding using local model."""

        def encode_text():
            embedding = self.local_model.encode(text)
            return embedding.tolist()

        return await asyncio.get_event_loop().run_in_executor(
            self.executor, encode_text
        )

    def _ensure_bucket_and_index(self):
        try:
            try:
                self.s3vectors_client.get_vector_bucket(
                    vectorBucketName=self.bucket_name
                )
                logger.info(f"Vector bucket '{self.bucket_name}' already exists")
            except Exception:
                # Create vector bucket
                logger.info(f"Creating vector bucket '{self.bucket_name}'...")
                self.s3vectors_client.create_vector_bucket(
                    vectorBucketName=self.bucket_name
                )
                logger.info(f"Created vector bucket '{self.bucket_name}'")

            # Check if index exists
            try:
                self.s3vectors_client.get_index(
                    vectorBucketName=self.bucket_name, indexName=self.index_name
                )
                logger.info(f"Vector index '{self.index_name}' already exists")
            except Exception:
                # Create vector index
                logger.info(f"Creating vector index '{self.index_name}'...")
                self.s3vectors_client.create_index(
                    vectorBucketName=self.bucket_name,
                    indexName=self.index_name,
                    dataType="float32",
                    dimension=self.vector_dimensions,
                    distanceMetric="cosine",  # Use cosine similarity like the original implementation
                )
                logger.info(f"Created vector index '{self.index_name}'")

        except Exception as e:
            logger.error(f"Error ensuring bucket and index: {e}")
            raise

    async def add_document(self, doc_data: Dict[str, Any]):
        """Add a document and its embedding to the store."""
        # Generate embedding for the document content
        embedding = await self._get_embedding(doc_data["content"])

        # Create unique key for the document (use URL hash or generate UUID)
        doc_key = str(hash(doc_data["url"]))

        # Prepare metadata (S3 Vectors supports metadata for filtering)
        metadata = {
            "title": doc_data["title"],
            "url": doc_data["url"],
            "section": doc_data.get("section", ""),
            "subsection": doc_data.get("subsection", ""),
            "content_preview": doc_data["content"][:200],  # First 200 chars for preview
        }

        # Store vector in S3 Vectors
        vector_data = {
            "key": doc_key,
            "data": {"float32": embedding},
            "metadata": metadata,
        }

        def put_vectors_sync():
            return self.s3vectors_client.put_vectors(
                vectorBucketName=self.bucket_name,
                indexName=self.index_name,
                vectors=[vector_data],
            )

        await asyncio.get_event_loop().run_in_executor(self.executor, put_vectors_sync)

        logger.debug(f"Added document to vector index: {doc_data['title']}")

    async def bulk_add_documents(
        self, documents: List[Dict[str, Any]], batch_size: int = 100
    ):
        """Add multiple documents in batches for efficiency."""
        logger.info(f"Adding {len(documents)} documents to S3 Vector Bucket store...")

        # Process documents in batches
        for i in range(0, len(documents), batch_size):
            batch = documents[i : i + batch_size]
            vectors_batch = []

            for doc_data in batch:
                # Generate embedding
                embedding = await self._get_embedding(doc_data["content"])

                # Create unique key
                doc_key = str(hash(doc_data["url"]))

                # Prepare metadata
                metadata = {
                    "title": doc_data["title"],
                    "url": doc_data["url"],
                    "section": doc_data.get("section", ""),
                    "subsection": doc_data.get("subsection", ""),
                    "content_preview": doc_data["content"][:200],
                }

                vectors_batch.append(
                    {
                        "key": doc_key,
                        "data": {"float32": embedding},
                        "metadata": metadata,
                    }
                )

            # Upload batch
            def put_vectors_batch_sync():
                return self.s3vectors_client.put_vectors(
                    vectorBucketName=self.bucket_name,
                    indexName=self.index_name,
                    vectors=vectors_batch,
                )

            await asyncio.get_event_loop().run_in_executor(
                self.executor, put_vectors_batch_sync
            )

            logger.info(
                f"Uploaded batch {i//batch_size + 1}/{(len(documents) + batch_size - 1)//batch_size}"
            )

        logger.info("Bulk document addition completed")

    async def search(
        self, query: str, limit: int = 5, section_filter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Search for similar documents using S3 Vectors native search."""
        # Generate query embedding
        query_embedding = await self._get_embedding(query)

        # Prepare metadata filter if section is specified
        metadata_filter = None
        if section_filter:
            metadata_filter = {"section": {"StringEquals": section_filter}}

        # Query S3 Vectors
        query_params = {
            "vectorBucketName": self.bucket_name,
            "indexName": self.index_name,
            "queryVector": {"float32": query_embedding},
            "topK": limit,
            "returnMetadata": True,
            "returnDistance": True,
        }

        if metadata_filter:
            query_params["metadataFilter"] = metadata_filter

        def query_vectors_sync():
            return self.s3vectors_client.query_vectors(**query_params)

        response = await asyncio.get_event_loop().run_in_executor(
            self.executor, query_vectors_sync
        )

        # Format results to match original API
        results = []
        for i, vector_result in enumerate(response.get("vectors", [])):
            metadata = vector_result.get("metadata", {})

            # Calculate similarity from distance (for cosine distance, similarity = 1 - distance)
            distance = vector_result.get("distance", 1.0)
            similarity = 1.0 - distance if distance is not None else 0.0

            result = {
                "id": i,
                "url": metadata.get("url", ""),
                "title": metadata.get("title", ""),
                "content": metadata.get("content_preview", ""),
                "section": metadata.get("section", ""),
                "subsection": metadata.get("subsection", ""),
                "similarity": float(similarity),
            }
            results.append(result)

        return results

    async def get_document_count(self) -> int:
        """Get the total number of documents in the store."""
        try:

            def list_vectors_sync():
                return self.s3vectors_client.list_vectors(
                    vectorBucketName=self.bucket_name,
                    indexName=self.index_name,
                    maxResults=1,  # We just want the total count
                )

            response = await asyncio.get_event_loop().run_in_executor(
                self.executor, list_vectors_sync
            )

            # If S3 Vectors provides a total count in the response, use it
            # Otherwise, we might need to paginate through all results
            return response.get("totalVectorCount", 0)

        except Exception as e:
            logger.error(f"Error getting document count: {e}")
            return 0

    async def get_bucket_info(self) -> Dict[str, Any]:
        """Get information about the S3 vector bucket and index."""
        try:

            def get_bucket_info_sync():
                try:
                    bucket_response = self.s3vectors_client.get_vector_bucket(
                        vectorBucketName=self.bucket_name
                    )
                except:
                    bucket_response = None

                try:
                    index_response = self.s3vectors_client.get_index(
                        vectorBucketName=self.bucket_name, indexName=self.index_name
                    )
                except:
                    index_response = None

                return bucket_response, index_response

            (
                bucket_response,
                index_response,
            ) = await asyncio.get_event_loop().run_in_executor(
                self.executor, get_bucket_info_sync
            )

            return {
                "bucket_name": self.bucket_name,
                "region": self.region,
                "index_name": self.index_name,
                "bucket_exists": bucket_response is not None,
                "vector_dimensions": self.vector_dimensions,
                "document_count": await self.get_document_count(),
                "bucket_info": bucket_response,
                "index_info": index_response,
                "embedding_method": "local",
                "embedding_model": self.embedding_model,
            }

        except Exception as e:
            logger.error(f"Error getting bucket info: {e}")
            return {
                "bucket_name": self.bucket_name,
                "region": self.region,
                "error": str(e),
            }

    async def close(self):
        """Close clients."""
        try:
            if self.s3vectors_client:
                self.s3vectors_client.close()
            if self.executor:
                self.executor.shutdown(wait=True)
        except Exception as e:
            logger.error(f"Error closing clients: {e}")

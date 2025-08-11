#!/usr/bin/env python3
"""
Script to scrape Materialize documentation and upload to S3 Vector Buckets.
Uses AWS S3 Vector Buckets native vector storage for efficient similarity search.
"""

import asyncio
import sys
from pathlib import Path
import os

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from materialize_mcp_server.search.doc_search import DocumentationSearcher
from materialize_mcp_server.search.s3_vector_bucket_store import S3VectorBucketStore


async def main():
    """Main function to scrape documentation and upload to S3 Vector Buckets."""
    print("=" * 70)
    print("Materialize Documentation S3 Vector Buckets Upload")
    print("=" * 70)
    
    # Get configuration from environment
    bucket_name = os.getenv("S3_VECTOR_BUCKET_NAME", "materialize-docs-vectors")
    region = os.getenv("AWS_REGION", "us-east-1")
    
    print("Configuration:")
    print(f"  S3 Vector Bucket: {bucket_name}")
    print(f"  AWS Region: {region}")
    print("  Embedding Method: Local Model (SentenceTransformers)")
    
    # Validate region support
    supported_regions = ["us-east-1", "us-east-2", "us-west-2", "ap-southeast-2", "eu-central-1"]
    if region not in supported_regions:
        print(f"\n⚠️  WARNING: Region '{region}' may not support S3 Vectors preview!")
        print(f"Supported regions: {', '.join(supported_regions)}")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting.")
            return
    
    # Check AWS credentials
    if not (os.getenv("AWS_ACCESS_KEY_ID", "AKIAV2KIV5LP5E5UBPHG") or os.getenv("AWS_PROFILE")):
        print("\n⚠️  WARNING: No AWS credentials found!")
        print("Please set one of:")
        print("  - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
        print("  - AWS_PROFILE environment variable")
        print("  - Configure AWS CLI with 'aws configure'")
        print()
        
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Exiting.")
            return
    
    try:
        # Initialize S3 Vector Bucket store
        print("\n🔧 Initializing S3 Vector Bucket store...")
        store = S3VectorBucketStore(
            bucket_name=bucket_name,
            region=region
        )

        # Get current document count
        count = await store.get_document_count()
        print(f"Current document count in S3 Vector Bucket: {count}")
        
        # Get bucket info
        bucket_info = await store.get_bucket_info()
        print(f"Vector bucket exists: {bucket_info.get('bucket_exists', False)}")
        print(f"Vector dimensions: {bucket_info.get('vector_dimensions', 'Unknown')}")
        print(f"Embedding method: {bucket_info.get('embedding_method', 'Unknown')}")
        
        if count > 0:
            print(f"\n📄 Found {count} existing documents in S3 Vector Bucket")
            response = input("Re-scrape and overwrite? (y/N): ")
            if response.lower() != 'y':
                print("Keeping existing documents.")
                await test_search(store)
                return
        
        # Create DocumentationSearcher to scrape docs (but don't initialize its store)
        print("\n🔍 Starting documentation scraping...")
        
        # We'll create a temporary searcher just for scraping
        temp_searcher = DocumentationSearcher()

        # Get the scraped documents without storing them in the old format
        print("Scraping documentation pages...")
        documents = []
        async for doc_batch in temp_searcher._scrape_documentation_generator(max_pages=500):
            documents.extend(doc_batch)
            print(f"Scraped {len(documents)} documents so far...")
        
        print(f"Scraped {len(documents)} total documents")
        
        # Upload to S3 Vector Buckets
        print(f"\n📤 Uploading {len(documents)} documents to S3 Vector Buckets...")
        await store.bulk_add_documents(documents)
        
        # Report final count
        final_count = await store.get_document_count()
        print(f"\n✅ Upload completed. Total documents in S3 Vector Bucket: {final_count}")
        
        # Test search functionality
        await test_search(store)
        
        print("\n🎉 Documentation is now stored in S3 Vector Buckets!")
        print("Enjoy native vector search with sub-second performance!")
        
        # Show usage instructions
        print("\n" + "=" * 70)
        print("Usage Instructions:")
        print("=" * 70)
        print("1. Configure your MCP server with:")
        print(f'   S3_VECTOR_BUCKET_NAME="{bucket_name}"')
        print(f'   AWS_REGION="{region}"')
        print()
        print("2. The documentation will be automatically loaded from S3 Vector Buckets")
        print("3. Native vector search with sub-second performance!")
        print()
        print("Cost Benefits:")
        print("- Up to 90% cost reduction compared to traditional vector databases")
        print("- Pay only for storage and queries (no infrastructure)")
        print("- Automatic optimization and scaling")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\nThis might be due to:")
        print("1. S3 Vectors not available in your region")
        print("2. Missing AWS permissions for S3 Vectors")
        print("3. S3 Vectors service not enabled in your account")
        print("\nPlease check the AWS S3 Vectors documentation for requirements.")
        raise
    
    finally:
        # Clean up
        if 'store' in locals():
            await store.close()


async def test_search(store: S3VectorBucketStore):
    """Test the search functionality."""
    print("\n🧪 Testing S3 Vector Buckets search functionality...")
    
    test_queries = [
        "How to create a materialized view?",
        "mz_now function usage",
        "PostgreSQL source configuration",
        "real-time data processing"
    ]
    
    for query in test_queries:
        print(f"\n  Query: '{query}'")
        try:
            results = await store.search(query, limit=2)
            if results:
                for i, result in enumerate(results, 1):
                    print(f"    {i}. {result['title']} (similarity: {result['similarity']:.3f})")
                    print(f"       {result['url']}")
            else:
                print("    No results found")
        except Exception as e:
            print(f"    Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
import logging
import elasticsearch

from typing import Optional

from langchain_core.documents import Document
from langchain_elasticsearch import ElasticsearchStore
from langchain_community.embeddings import InfinityEmbeddings
from open_webui.apps.retrieval.vector.main import VectorItem, SearchResult, GetResult
from open_webui.config import (
    ELASTICSEARCH_URI
)


log = logging.getLogger(__name__)

DEFAULT_INDEX_NAME = "rag-default-idx"


'''
`Elasticsearch` do not have `collection` data structure.
But in order to implement the vectorstore interface correctly.
We will need to use an alternative way to provided the functionality.
Therefore, we will take `collection_name` as a metadata tag of `elasticsearch` index data structure.
For example,
Given 10 documents with every document have 8 chunks.

`ChromaDB` and `Milvus` with `collection` data structure:
they will create 10 collections, and every collection have 8 chunk of docs

`Elasticsearch` without `collection` data structure:
we will have only single one index, and within it, we will create 80 chunks.
But for those chunks that come from the same document, it `collection` metadata tag will have the same one.
'''
class ElasticsearchClient:
    def __init__(self):
        self.client = elasticsearch.ElasticSearch(
            hosts=[ELASTICSEARCH_URI]
        )
        self.embeddings = InfinityEmbeddings(
            model="BAAI/bge-m3",
            infinity_api_url="http://10.0.0.196:7997",
        )
        self.es_vs = ElasticsearchStore(
            es_connection=self.client,
            index_name=DEFAULT_INDEX_NAME,
            embedding=embeddings
        )
        

    def _hits_to_get_result(self, hits) -> GetResult:
        ids = []
        documents = []
        metadatas = []

        for hit in hits:
            ids.append(hit['_source']['id'])
            documents.append(hit['_source']['text'])
            metadatas.append(hit['_source']['metadata'])

        return GetResult(
            **{
                "ids": ids,
                "documents": documents,
                "metadatas": metadatas,
            }
        )

    def has_collection(self, collection_name: str) -> bool:
        query = {
            "query": {
                "term": {
                    "metadata.collection": collection_name
                }
            }
        }
        response = self.client.search(index=DEFAULT_INDEX_NAME, body=query)

        if response["hits"]["total"]["value"] > 0:
            return True
        else:
            return False

    def delete_collection(self, collection_name: str):
        # Delete the collection based on the collection name.
        query = {
            "query": {
                "term": {
                    "metadata.collection": collection_name
                }
            }
        }
        response = client.delete_by_query(index=DEFAULT_INDEX_NAME, body=query)

        deleted_count = response.get('deleted', 0)
        log.info(f"number of deleted document: {deleted_count}")

    def search(
        self, collection_name: str, vectors: list[list[float | int]], limit: int
    ) -> Optional[SearchResult]:
        # Search for the nearest neighbor items based on the vectors and return 'limit' number of results.
        return NotImplemented

    def query(
        self, collection_name: str, filter: dict, limit: Optional[int] = None
    ) -> Optional[GetResult]:
        # Query the items from the collection based on the filter.
        return NotImplemented

    def get(self, collection_name: str) -> Optional[GetResult]:
        query = {
            "query": {
                "term": {
                    "metadata.collection": collection_name
                }
            }
        }
        response = self.client.search(index=DEFAULT_INDEX_NAME, body=query)

        print(response)
        return self._hits_to_get_result(response['hits']['hits'])

    def insert(self, collection_name: str, items: list[VectorItem]):
        # Insert the items into the collection, if the collection does not exist, it will be created. (Original)
        docs = [
            Document(
                page_content=item["text"],
                metadata={
                    **item["metadata"],
                    "collection": collection_name
                }
            ) for item in items
        ]

        ids = [
            item["id"] for item in items
        ]

        self.client.add_documents(documents=documents, ids=ids)

    def upsert(self, collection_name: str, items: list[VectorItem]):
        # Update the items in the collection, if the items are not present, insert them. If the collection does not exist, it will be created.
        self.delete_collection(collection_name)
        self.insert(collection_name, items)

    def delete(
        self,
        collection_name: str,
        ids: Optional[list[str]] = None,
        filter: Optional[dict] = None,
    ):
        self.delete_collection(collection_name)

    def reset(self):
        # Resets the database. This will delete all collections and item entries.
        response = self.client.indices.delete(index=DEFAULT_INDEX_NAME, ignore=[400, 404])

        # Check the response
        if response.get('acknowledged', False):
            print(f"Index '{index_name}' deleted successfully.")
        else:
            print(f"Failed to delete index '{index_name}'.")

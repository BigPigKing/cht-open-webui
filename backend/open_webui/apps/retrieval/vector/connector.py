from open_webui.config import VECTOR_DB

if VECTOR_DB == "milvus":
    from open_webui.apps.retrieval.vector.dbs.milvus import MilvusClient

    VECTOR_DB_CLIENT = MilvusClient()
elif VECTOR_DB == "elasticsearch":
    from open_webui.apps.retrieval.vector.dbs.elasticsearch import ElasticsearchClient

    VECTOR_DB_CLIENT = ElasticsearchClient()
else:
    from open_webui.apps.retrieval.vector.dbs.chroma import ChromaClient

    VECTOR_DB_CLIENT = ChromaClient()

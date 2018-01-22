package ucm.socialbigdata.com.elasticsearch

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

class SimpleElasticsearchSink(elastic_index: String, elastic_type: String) extends  ElasticsearchSinkFunction[String]{

  def createIndexRequest(element: String): IndexRequest = {
    Requests.indexRequest.index(elastic_index).
      `type`(elastic_type).source(element)
  }

  override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
    indexer.add(createIndexRequest(element))
  }
}


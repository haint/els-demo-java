package els.sync.demo;

import java.io.IOException;
import java.math.BigInteger;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.web3j.protocol.core.methods.response.Transaction;

/**
 * @author Ethan Hunt
 *
 * @created Sep 19, 2018
 */

public class Utils {
  
  public static boolean enough(RestHighLevelClient elsClient, int transactionsLength, BigInteger blockNumber) throws IOException {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.termQuery("blockNumber", blockNumber.intValue()));
    sourceBuilder.size(0);

    SearchRequest searchRequest = new SearchRequest("eth_trans");
    searchRequest.source(sourceBuilder);

    SearchResponse response = elsClient.search(searchRequest, RequestOptions.DEFAULT);
    return transactionsLength == response.getHits().totalHits;
  }

  public static boolean existedBlock(RestHighLevelClient elsClient, BigInteger blockNumber) throws IOException {
    GetRequest getRequest = new GetRequest("eth_blocks", "_doc", blockNumber.toString());
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    getRequest.storedFields("_none_");
    return elsClient.exists(getRequest, RequestOptions.DEFAULT);
  }

  public static boolean existedTransaction(RestHighLevelClient elsClient, Transaction transaction) throws IOException {
    GetRequest getRequest = new GetRequest("eth_trans", "_doc", transaction.getHash());
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    getRequest.storedFields("_none_");
    return elsClient.exists(getRequest, RequestOptions.DEFAULT);
  }

  public static void indexBlock(RestHighLevelClient elsClient, int transactionsLength, BigInteger blockNumber, String hash) throws IOException {
    IndexRequest indexRequest = new IndexRequest("eth_blocks", "_doc", blockNumber.toString());
    indexRequest.source("transactions", transactionsLength, "hash", hash);
    elsClient.index(indexRequest, RequestOptions.DEFAULT);
  }
}

package els.sync.demo;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.utils.Convert;
import org.web3j.utils.Convert.Unit;

/**
 * @author Ethan Hunt
 *
 * @created Sep 17, 2018
 */

public class ELSIndexWorker implements Runnable {
  
  private final Web3j ethClient;
  
  private final RestHighLevelClient elsClient;
  
  private final BigInteger blockNumber;
  
  public ELSIndexWorker(Web3j ethClient, RestHighLevelClient elsClient, BigInteger blockNumber) {
    this.ethClient = ethClient;
    this.elsClient = elsClient;
    this.blockNumber = blockNumber;
  }

  @Override
  public void run() {
    try {
      Block block = ethClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(blockNumber), true).send().getBlock();
      if (block == null) return;
      
      List<TransactionResult> transactions = block.getTransactions();
      int transactionLength = transactions.size();
      
      if (existedBlock(blockNumber)) return;
      
      if (transactions.size() == 0) {
        indexBlock(transactionLength, blockNumber, block.getHash());
        return;
      }
      
      if (enough(transactions.size(), blockNumber)) {
        indexBlock(transactionLength, blockNumber, block.getHash());
        return;
      }
      
      long start = System.currentTimeMillis();
      for (TransactionResult<TransactionObject> result : transactions) {
        TransactionObject obj = result.get();
        Transaction transaction = obj.get();
        indexTransactions(transaction, block);
      }
      
      indexBlock(transactionLength, blockNumber, block.getHash());
      
      System.out.println(new Date() + "| Index " + transactionLength +" transactions of block " + blockNumber + " in " + (System.currentTimeMillis() - start) + "(ms)");
      
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  private void indexTransactions(Transaction transaction, Block block) throws IOException {
    
    if (existedTransaction(transaction)) return;
    
    IndexRequest indexRequest = new IndexRequest("eth_trans", "_doc", transaction.getHash());
    
    String jsonString = "{\n"
          + "  \"blockNumber\":" + block.getNumber().longValue() + ",\n"
//          + "  \"blockHash\":\"" + block.getHash() + "\",\n"
          + "  \"input\":\"" + transaction.getInput() + "\",\n"
          + "  \"from\":\"" + transaction.getFrom() + "\",\n"
          + "  \"to\":\"" + transaction.getTo() + "\",\n"
          + "  \"value\":" + Convert.fromWei(new BigDecimal(transaction.getValue()), Unit.ETHER).toString() + ",\n"
//          + "  \"gas\":" + transaction.getGas().toString() + ",\n"
//          + "  \"gasPrice\":" + Convert.fromWei(new BigDecimal(transaction.getGasPrice()), Unit.GWEI).toString() + ",\n"
          + "  \"timestamp\":" + block.getTimestamp().longValue() + "\n"
        + "}";
    
    indexRequest.source(jsonString, XContentType.JSON);
    
    elsClient.index(indexRequest, RequestOptions.DEFAULT);
  }
  
  private boolean enough(int transactionsLength, BigInteger blockNumber) throws IOException {
    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
    sourceBuilder.query(QueryBuilders.termQuery("blockNumber", blockNumber.intValue()));
    sourceBuilder.size(0);
    
    SearchRequest searchRequest = new SearchRequest("eth_trans");
    searchRequest.source(sourceBuilder);
    
    SearchResponse response = elsClient.search(searchRequest, RequestOptions.DEFAULT);
    return transactionsLength == response.getHits().totalHits;
  }

  private boolean existedBlock(BigInteger blockNumber) throws IOException {
    GetRequest getRequest = new GetRequest("eth_blocks", "_doc", blockNumber.toString());
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    getRequest.storedFields("_none_");
    return elsClient.exists(getRequest, RequestOptions.DEFAULT);
  }
  
  private boolean existedTransaction(Transaction transaction) throws IOException {
    GetRequest getRequest = new GetRequest("eth_trans", "_doc", transaction.getHash());
    getRequest.fetchSourceContext(new FetchSourceContext(false));
    getRequest.storedFields("_none_");
    return elsClient.exists(getRequest, RequestOptions.DEFAULT);
  }
  
  private void indexBlock(int transactionsLength, BigInteger blockNumber, String hash) throws IOException {
    IndexRequest indexRequest = new IndexRequest("eth_blocks", "_doc", blockNumber.toString());
    indexRequest.source("transactions", transactionsLength, "hash", hash);
    elsClient.index(indexRequest, RequestOptions.DEFAULT);
  }
}

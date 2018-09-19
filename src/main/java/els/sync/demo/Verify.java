package els.sync.demo;

import java.math.BigInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

/**
 * @author Ethan Hunt
 *
 * @created Sep 19, 2018
 */

public class Verify {

  public static void main(String[] args) throws Exception {
    Web3j ethClient = Web3j.build(new HttpService("https://mainnet.infura.io/v3/b48465719cae4527b984c1b2a5767c3e"));
    RestHighLevelClient elsClient = new RestHighLevelClient(RestClient.builder(new HttpHost("els-demo", 9200, "http")));
    
    int blockNumber = ethClient.ethBlockNumber().send().getBlockNumber().intValue();
    
    int pageSize = 100 * 1000;
    int totalPage = blockNumber / pageSize;
    
    if (blockNumber % pageSize != 0) totalPage++;
    
    int currentPage = 0;
    
    ExecutorService executorService = Executors.newFixedThreadPool(totalPage);
    
    while (currentPage != totalPage) {
      int start = currentPage * pageSize;
      int end = (++currentPage) * pageSize;
      if (end > blockNumber) end = blockNumber;
      
      System.out.println("From: " + start + " - To: " + end);
      executorService.submit(new BatchIndex(ethClient, elsClient, start, end));
    }
  }
  
  public static class BatchIndex implements Runnable {
    
    private final int start;
    
    private final int end;
    
    private final Web3j ethClient;
    
    private final RestHighLevelClient elsClient;
    
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    
    public BatchIndex(Web3j ethClient, RestHighLevelClient elsClient, int start, int end) {
      this.start = start;
      this.end = end;
      this.elsClient = elsClient;
      this.ethClient = ethClient;
    }

    @Override
    public void run() {
      for (int i = start; i < end; i++) {
        final int block = i;
        executorService.submit(new ELSIndexWorker(ethClient, elsClient, BigInteger.valueOf(block)));
      }
    }
  }
}

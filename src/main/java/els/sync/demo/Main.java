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
 * @created Sep 17, 2018
 */

public class Main {

  public static void main(String[] args) throws Exception {
    
    Web3j ethClient = Web3j.build(new HttpService("https://mainnet.infura.io/v3/b48465719cae4527b984c1b2a5767c3e"));
    RestHighLevelClient elsClient = new RestHighLevelClient(RestClient.builder(new HttpHost("els-demo", 9200, "http")));
    BigInteger blockNumber = ethClient.ethBlockNumber().send().getBlockNumber();
    
    System.out.println(blockNumber);
    
//    ethClient.blockObservable(true).subscribe(block -> {
//      System.out.println(block.getBlock().getNumber());
//    });
    
//    new ELSIndexWorker(ethClient, elsClient, blockNumber).run();
    
    ExecutorService executorService = Executors.newFixedThreadPool(100);
//    
    for (BigInteger bi = BigInteger.ZERO; bi.compareTo(blockNumber) <= 0 ; bi = bi.add(BigInteger.ONE)) {
      executorService.submit(new ELSIndexWorker(ethClient, elsClient, bi));
    }
//    
//    ELSIndexWorker worker = new ELSIndexWorker(BigInteger.valueOf(1));
//    worker.run();
    
//    Web3j web3 = Web3j.build(new HttpService("https://mainnet.infura.io/v3/b48465719cae4527b984c1b2a5767c3e"));
//    Web3ClientVersion version = web3.web3ClientVersion().send();
//    System.out.println(version.getResult());
//    
//    BigInteger blockNumber = web3.ethBlockNumber().send().getBlockNumber();
//    System.out.println(blockNumber);
//    
//    RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("els-demo", 9200, "http")));
//    System.out.println(client);
//    
//    GetIndexRequest request = new GetIndexRequest();
//    request.indices("eth_blocks");
//    
//    boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
//    
//    System.out.println(exists);
//    
//    GetRequest getRequest = new GetRequest("eth_blocks", "doc", blockNumber.toString());
//    getRequest.fetchSourceContext(new FetchSourceContext(false));
//    getRequest.storedFields("_none_");
//    
//    exists = client.exists(getRequest, RequestOptions.DEFAULT);
//    
//    System.out.println(exists);
//    
//    SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//    sourceBuilder.query(QueryBuilders.termQuery("blockNumber", 2000733));
//    sourceBuilder.size(0);
//    
//    SearchRequest searchRequest = new SearchRequest("eth_trans");
//    searchRequest.source(sourceBuilder);
//    
//    SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
//    System.out.println(response.getHits().totalHits);
//    
//    System.exit(0);
//    ExecutorService executorService = Executors.newFixedThreadPool(10);
//    
//    for (BigInteger bi = blockNumber; bi.compareTo(BigInteger.ZERO) >= 0 ; bi = bi.subtract(BigInteger.ONE)) {
//      final BigInteger number = bi;
//      
//      executorService.submit(new Runnable() {
//        @Override
//        public void run() {
//          try {
//            EthBlock block = web3.ethGetBlockByNumber(DefaultBlockParameter.valueOf(number), true).send();
//            System.out.println(number);
//            List<TransactionResult> transactions = block.getBlock().getTransactions();
//            System.out.println(transactions.size());
//            for (TransactionResult result : transactions) {
//              TransactionObject obj = (TransactionObject) result.get();
//              Transaction transaction = obj.get();
//              
//              System.out.println(transaction.getHash());
//              System.out.println(transaction.getValue());
//              System.out.println(Convert.fromWei(new BigDecimal(transaction.getValue()), Unit.ETHER));
//              System.out.println(transaction.getValueRaw());
//              System.out.println("----------------------------------------");
//            }
//          } catch (IOException e) {
//            e.printStackTrace();
//          }
//        }
//      });
      
//    }
  }
}

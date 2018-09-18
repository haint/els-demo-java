package els.sync.demo;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    
    if (args.length == 0) {
      BigInteger blockNumber = ethClient.ethBlockNumber().send().getBlockNumber();
      ExecutorService executorService = Executors.newFixedThreadPool(100);
      for (BigInteger bi = blockNumber; bi.compareTo(BigInteger.ZERO) >= 0 ; bi = bi.subtract(BigInteger.ONE)) {
        executorService.submit(new ELSIndexWorker(ethClient, elsClient, bi));
      }
    } else if (args[0].equals("sync")) {
      ScheduledExecutorService execService = Executors.newScheduledThreadPool(10);
      execService.scheduleAtFixedRate(new Runnable() {
        
        @Override
        public void run() {
          try {
            BigInteger blockNumber = ethClient.ethBlockNumber().send().getBlockNumber();
            System.out.println(new Date() + "| Highest block: " + blockNumber);
            
            for (BigInteger bi = blockNumber; bi.compareTo(blockNumber.subtract(BigInteger.valueOf(10))) >= 0 ; bi = bi.subtract(BigInteger.ONE)) {
              new Thread(new ELSIndexWorker(ethClient, elsClient, bi)).start();
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        
      }, 0, 10, TimeUnit.SECONDS);
    }
  }
}

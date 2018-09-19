package els.sync.demo;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
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
      
      if (Utils.existedBlock(elsClient, blockNumber)) return;
      
      if (transactions.size() == 0) {
        Utils.indexBlock(elsClient, transactionLength, blockNumber, block.getHash());
        return;
      }
      
      if (Utils.enough(elsClient, transactions.size(), blockNumber)) {
        Utils.indexBlock(elsClient, transactionLength, blockNumber, block.getHash());
        return;
      }
      
      long start = System.currentTimeMillis();
      for (TransactionResult<TransactionObject> result : transactions) {
        TransactionObject obj = result.get();
        Transaction transaction = obj.get();
        indexTransactions(transaction, block);
      }
      
      Utils.indexBlock(elsClient, transactionLength, blockNumber, block.getHash());
      
      System.out.println(new Date() + "| Index " + transactionLength +" transactions of block " + blockNumber + " in " + (System.currentTimeMillis() - start) + "(ms)");
      
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  private void indexTransactions(Transaction transaction, Block block) throws IOException {
    
    if (Utils.existedTransaction(elsClient, transaction)) return;
    
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
}

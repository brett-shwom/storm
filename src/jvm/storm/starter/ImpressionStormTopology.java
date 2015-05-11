package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Random;
import java.util.Map;
import java.util.HashMap;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.util.concurrent.Future;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.async.Callback;
import java.util.concurrent.ExecutorService;

//How do you test Storm Topologies?

class ImpressionTopology {

  static TopologyBuilder getBuilder() {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("impressionCid", new RandomImpressionSpout(), 10);
    builder.setBolt("cidCount", new ImpressionCounterBolt(), 3).fieldsGrouping("impressionCid", new Fields("cid"));
    builder.setBolt("impressionCap", new ImpressionCapBolt(), 3).shuffleGrouping("cidCount");
    builder.setBolt("impressionCapNotification", new ImpressionCapNotificationBolt(),2).shuffleGrouping("impressionCap");


    return builder;

  }

  public static void main(String[] args) throws Exception {
   
    TopologyBuilder builder = getBuilder();

    Config conf = new Config();
    conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("test", conf, builder.createTopology());
    Utils.sleep(10000);
    cluster.killTopology("test");
    cluster.shutdown();
    
  }
}

// class ImpressionCountPersistenceBolt extends BaseRichBolt {
//   static final TABLE_NAME = "impressionsByCid";

//   OutputCollector _collector;

//   @Override
//   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
//     _collector = collector;
//   }

//   @Override
//   public void execute(Tuple tuple) {

//     _collector.ack(tuple);
//   }

//   public String buildUpdateCommand(String cid, Long impressions) {
//     return "UPDATE users SET city= 'San Jose' WHERE lastname= 'Doe';";
//   }

// }


class ImpressionCapNotificationBolt extends BaseRichBolt {
  OutputCollector _collector;

  static final Long MOD = 1000L;
  ExecutorService _sharedExecutor = null;

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    _sharedExecutor = context.getSharedExecutor();
  }

  @Override
  public void execute(Tuple tuple) {

    final String cid = tuple.getString(0);
    final Long impressionCount = tuple.getLong(1);
    final Long impressionCap = tuple.getLong(2);

    if (impressionCount < impressionCap) {
      //this shouldnt happen
    }
    else {
      if ((impressionCount - impressionCap) % MOD == 0L) {
        makeRequest(cid,impressionCount,impressionCap);
      }
    }
    _collector.ack(tuple);
  }

  public void makeRequest(final String cid, final Long impressionCount, final Long impressionCap) {

    final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(ImpressionCapNotificationBolt.class);
    LOG.info("Making request");

    //Is this the right way to make async calls in a bolt?
    Future<Void> future = _sharedExecutor.submit(new java.util.concurrent.Callable<Void>() {

      @Override
      public Void call() throws Exception {
        Future<HttpResponse<JsonNode>> future = Unirest.post("http://localhost:8080/impressionCap")
          .header("accept", "application/json")
          .header("Content-Type", "application/json")
          .body("{\"cid\":\"" + cid + "\",\"impressionCount\":" + impressionCount + ",\"impressionCap\":" + impressionCap + "}")
          // .field("cid", cid)
          // .field("impressionCount", impressionCount)
          // .field("impressionCap", impressionCount)
          .asJsonAsync(new Callback<JsonNode>() {

            public void failed(UnirestException e) {
              LOG.info("The request has failed");
            }

            public void completed(HttpResponse<JsonNode> response) {
              LOG.info("The request was completed successfully");
            }

            public void cancelled() {
              LOG.info("The request was cancelled");
            }

          });
        return null;
      }
    });

  




  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}

class ImpressionCapBolt extends BaseRichBolt {
  OutputCollector _collector;

  //for testing purposes, I'm hardcoding the impression caps.
  //Will this Bolt have a periodic task that pings the db for impression caps and updates the impressionCapPerId Map?
  //Or should there be a spout which provides updated impression caps to which the ImpressionCapBolt is linked via an edge?
  static final Map<String, Long> impressionCapPerCid;
  static {
    impressionCapPerCid = new HashMap<String, Long>();
    impressionCapPerCid.put("1", 1L);
    impressionCapPerCid.put("2", 1L);
    impressionCapPerCid.put("3", 1L);
    impressionCapPerCid.put("4", 1L);
    impressionCapPerCid.put("5", 1L);
  }


  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {

    String cid = tuple.getString(0);
    Long impressionCount = tuple.getLong(1);

    Long impressionCap = impressionCapPerCid.get(cid);

    if (impressionCap == null) {
      //TODO: what should we do in this case - i.e. we don't have an impression cap for this cid
    }
    else {
      if (impressionCount >= impressionCap) {
        _collector.emit(tuple, new Values(cid, impressionCount,impressionCap));
      }
    }

    _collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cid", "impressionCount","impressionCap"));
  }

}

class ImpressionCounterBolt extends BaseRichBolt {
  OutputCollector _collector;

  Map<String, Long> impressionsPerCid = new HashMap<String, Long>(); 
  //What happens if the task dies, then the impressionsPerCids will be lost, right? How do we prevent this?

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {

    String cid = tuple.getString(0);

    Long impressions = impressionsPerCid.get(cid);

    if (impressions == null) {
      impressions=1L;
    }
    else {
      impressions+=1L;
    }

    impressionsPerCid.put(cid, impressions);

    _collector.emit(tuple, new Values(cid,impressions));
    _collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cid", "impressionCount"));
  }


}

//Does there exist a Jetstream spout? if so, is it a reliable spout?
class RandomImpressionSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(1000);
    final String[] cids = new String[]{ "1", "2", "3", "4", "5" };
    final String cid = cids[_rand.nextInt(cids.length)];
    _collector.emit(new Values(cid));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("cid"));
  }

}
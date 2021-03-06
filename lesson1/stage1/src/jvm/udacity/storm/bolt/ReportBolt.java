package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import udacity.storm.tools.Rankable;
import udacity.storm.tools.Rankings;

import java.util.Map;

public class ReportBolt extends BaseRichBolt{
    /**
     * A bolt that prints the word and count to redis
     */
      // place holder to keep the connection to redis
      transient RedisConnection<String,String> redis;

      @Override
      public void prepare(
              Map map,
              TopologyContext topologyContext,
              OutputCollector outputCollector)
      {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost",6379);

        // initiate the actual connection
        redis = client.connect();
      }

      @Override
      public void execute(Tuple tuple) {
          // access the first column 'word'

          String word = (String) tuple.getValue(0);

          //access the second column 'count'
          redis.publish("WordCountTopology", word + "|" + Long.toString(30));

          //publish the word count to redis using word as the key
      }

      public void declareOutputFields(OutputFieldsDeclarer declarer)
      {
        // nothing to add - since it is the final bolt
      }
}

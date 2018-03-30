package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ParseTweetBolt extends BaseRichBolt{
    /**
     * A bolt that parses the tweet into words
     */
      // To output tuples from this bolt to the count bolt
      OutputCollector collector;
      static private List<String> filterList = Arrays.asList("https://t","de","you","que","in","and","to","I","of");

      @Override
      public void prepare(
              Map map,
              TopologyContext topologyContext,
              OutputCollector         outputCollector)
      {
        // save the output collector for emitting tuples
        collector = outputCollector;
      }

      @Override
      public void execute(Tuple tuple)
      {
        // get the 1st column 'tweet' from tuple
        String tweet = tuple.getString(0);

        // provide the delimiters for splitting the tweet
        String delims = "[ .,?!]+";

        // now split the tweet into tokens
        String[] tokens = tweet.split(delims);

        // for each token/word, emit it
        for (String token: tokens) {
          //if (!filterList.contains(token) && (token.length() > 4) && (token.charAt(0)=='#')){
            if (token.startsWith("#") && token.length() > 3 && !token.equals("") && token.matches("^[#a-zA-Z0-9]*$")){
              collector.emit(new Values(token));
          }
        }
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer declarer)
      {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet-word'
        declarer.declare(new Fields("tweet-word"));
      }
}

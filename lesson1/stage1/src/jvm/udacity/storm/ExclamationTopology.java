package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import udacity.storm.bolt.ReportBolt;
import udacity.storm.spout.MyLikesSpout;
import udacity.storm.spout.MyNamesSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */

/**
 * This is a basic example of a storm topology.
 *
 * This topology demonstrates how to add three exclamation marks '!!!'
 * to each word emitted
 *
 * This is an example for Udacity Real Time Analytics Course - ud381
 *
 */
public class ExclamationTopology {

  /**
   * A bolt that adds the exclamation marks '!!!' to word
   */
  public static class ExclamationBolt extends BaseRichBolt
  {
    // To output tuples from this bolt to the next stage bolts, if any
    OutputCollector _collector;
    HashMap<String,String> favoritesMap;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      // save the output collector for emitting tuples
      _collector = collector;
      favoritesMap = new HashMap<String, String>();
    }

    @Override
    public void execute(Tuple tuple)
    {
      // get the column word from tuple
      String componentId = tuple.getSourceComponent();
      String sentence;

      if (componentId.equals("my-likes-spout")){
        // Tuple fields = (Tuple) tuple.getValue(0);
        String name= (String)tuple.getValue(0);

        if(!favoritesMap.containsKey(name)){
          favoritesMap.put(name,(String) tuple.getValue(1));
        }
      }else if(componentId.equals("my-names-spout")){
        String name = tuple.getString(0);

        if (favoritesMap.containsKey(name)){
          sentence = name+"'s favorite is "+favoritesMap.get(name)+"!!!";

          _collector.emit(tuple, new Values(sentence));
        }
      }else if (componentId.equals("exclaim1")){
        sentence = (String) tuple.getValue(0);

         _collector.emit(tuple, new Values(sentence+"!!!"));
      }

      // build the word with the exclamation marks appended


      // emit the word with exclamations

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // tell storm the schema of the output tuple for this spout

      // tuple consists of a single column called 'exclamated-word'
      declarer.declare(new Fields("exclamated-word"));
    }
  }

  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    // attach the word spout to the topology - parallelism of 10
    builder.setSpout("my-likes-spout", new MyLikesSpout(), 10);

    builder.setSpout("my-names-spout", new MyNamesSpout(), 10);

    // attach the exclamation bolt to the topology - parallelism of 3
    builder.setBolt("exclaim1", new ExclamationBolt(), 3)
            .shuffleGrouping("my-likes-spout")
            .shuffleGrouping("my-names-spout");

    // attach another exclamation bolt to the topology - parallelism of 2
    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    builder.setBolt("report-bolt", new ReportBolt(),1).globalGrouping("exclaim2");

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("exclamation", conf, builder.createTopology());

      // let the topology run for 20 seconds. note topologies never terminate!
      Thread.sleep(20000);

      // kill the topology
      cluster.killTopology("exclamation");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}

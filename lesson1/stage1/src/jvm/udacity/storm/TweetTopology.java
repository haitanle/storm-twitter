package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import udacity.storm.bolt.*;
import udacity.storm.spout.TweetSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class TweetTopology {


  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
            "hIPKnoQHqS31vtvRRNRI4MiVZ",
            "Ej7a3S4QcoVhdI6FyXoMf1cm2sZlqxZd5d1qHsucLepHjipOYh",
            "172153182-ec2vJOZYqewW1fFZTHshmXIq5QLRZ2tx4QLRcOhg",
            "SXLwhjJjSJ8nEVrAtL5OONxNDrUOoVM1meSfLDZSv8fgL"
    );


    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout("tweet-spout", tweetSpout, 1);

    //*********************************************************************
    // Complete the Topology.
    // Part 1: // attach the parse tweet bolt, parallelism of 10 (what grouping is needed?)
    builder.setBolt("python-url-bolt", new URLBolt(),10).shuffleGrouping("tweet-spout");

    builder.setBolt("python-split-sentence", new SplitSentence(),10).shuffleGrouping("python-url-bolt");

    // Part 2: // attach the count bolt, parallelism of 15 (what grouping is needed?)
    builder.setBolt("rolling-count-bolt", new RollingCountBolt(40,20), 15).fieldsGrouping("python-split-sentence",new Fields("word"));
    // Part 3: attach the report bolt, parallelism of 1 (what grouping is needed?)
    builder.setBolt("report-bolt", new ReportBolt(),1).globalGrouping("rolling-count-bolt");
    // Submit and run the topology.


    //*********************************************************************


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

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 30 seconds. note topologies never terminate!
      Utils.sleep(1000*30000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}

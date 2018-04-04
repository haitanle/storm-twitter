package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import udacity.storm.tools.Rankable;
import udacity.storm.tools.Rankings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopHashTagBolt extends BaseRichBolt
{
    // To output tuples from this bolt to the next stage bolts, if any
    OutputCollector _collector;
    List<String> topHashTags;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector         collector)
    {
        // save the output collector for emitting tuples
        _collector = collector;
        topHashTags = new ArrayList<String>();
    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the column word from tuple
        String componentId = tuple.getSourceComponent();

        if (componentId.equals("total-ranking-bolt")){
            Rankings rankList = (Rankings) tuple.getValue(0);
            for (Rankable r : rankList.getRankings()) {
                if (!topHashTags.contains((String)r.getObject())){
                    topHashTags.add((String)r.getObject());
                }
            }
            }
        }

        }






        String sentence;

        if (componentId.equals("my-likes-spout")){
            // Tuple fields = (Tuple) tuple.getValue(0);
            String name= (String)tuple.getValue(0);

            if(!topHashTags.containsKey(name)){
                topHashTags.put(name,(String) tuple.getValue(1));
            }
        }else if(componentId.equals("my-names-spout")){
            String name = tuple.getString(0);

            if (topHashTags.containsKey(name)){
                sentence = name+"'s favorite is "+topHashTags.get(name)+"!!!";

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
        declarer.declare(new Fields("tweets"));
    }





    //Rankings rankList = (Rankings) tuple.getValue(0);

          for (Rankable r : rankList.getRankings()) {
    if (r != null) {
        String word = r.getObject().toString();
        long count = r.getCount();
        redis.publish("WordCountTopology", word + "|" + Long.toString(count));
    }
package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.*;

public class TopHashTagBolt extends BaseRichBolt
{
    // To output tuples from this bolt to the next stage bolts, if any
    OutputCollector _collector;
    private static Map<String, Integer> allHashTagsMap;
    private static LinkedHashMap<String, Integer> topHashTags;
    int minCount;
    private final static int TOPN = 5;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector         collector)
    {
        // save the output collector for emitting tuples
        _collector = collector;

        try {
            FileInputStream fileIn = new FileInputStream("allHashTags.txt");
            ObjectInputStream in = new ObjectInputStream(fileIn);

            this.allHashTagsMap = (HashMap<String, Integer>) in.readObject();

//            in.close();
//            fileIn.close();
        } catch (IOException i) {
            i.printStackTrace();
            return;
        } catch (ClassNotFoundException c) {
            System.out.println("Employee class not found");
            c.printStackTrace();
            return;
        }






        allHashTagsMap = new HashMap<String, Integer>();

        topHashTags = new LinkedHashMap<String, Integer>();
        minCount = 0;
    }

    @Override
    public void execute(Tuple tuple)
    {
        // get the column word from tuple
        String componentId = tuple.getSourceComponent();

        if (componentId.equals("count-bolt")){
            String hashTag = (String) tuple.getValue(0);
            Integer hashCount = (Integer) tuple.getValue(1);

            this.allHashTagsMap.put(hashTag,hashCount);
            //if (hashCount>minCount){
            sortHashMap(this.allHashTagsMap,TOPN);
            for (Map.Entry<String,Integer> entry: topHashTags.entrySet()) {
                _collector.emit(new Values(entry.getKey() + " " + entry.getValue()));
            }
            //}
        }
        else if (componentId.equals("tweet-spout")){
            String tweet = tuple.getString(0);
            for (Map.Entry<String,Integer> entry: topHashTags.entrySet()){
                if (tweet.contains(entry.getKey())){
                    _collector.emit(new Values(entry.getKey()+" "+tweet));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // tell storm the schema of the output tuple for this spout

        // tuple consists of a single column called 'tweet'
        declarer.declare(new Fields("tweets"));
    }

    private void sortHashMap(Map<String, Integer> unsortedMap,int size){

        this.topHashTags.clear();

        Comparator<Map.Entry<String,Integer>> valueComparator = new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                Integer v1 = o1.getValue();
                Integer v2 = o2.getValue();
                return v1.compareTo(v2);
            }
        };

        //Get each entry of the hashMap
        Set<Map.Entry<String, Integer>> entries = unsortedMap.entrySet();

        // Sort method needs a List, so let's first convert Set to List in Java
        List<Map.Entry<String, Integer>> listOfEntries = new ArrayList<Map.Entry<String, Integer>>(entries);

        //Perform storing
        Collections.sort(listOfEntries,valueComparator);

        // copying entries from List back to Map

        for (Map.Entry<String, Integer> entry: listOfEntries.subList(0,size)){
            if (entry.getValue()<0){
                this.minCount = entry.getValue();
            }
            topHashTags.put(entry.getKey(),entry.getValue());
        }
    }

}





//
//        String sentence;
//
//        if (componentId.equals("my-likes-spout")){
//            // Tuple fields = (Tuple) tuple.getValue(0);
//            String name= (String)tuple.getValue(0);
//
//            if(!topHashTags.containsKey(name)){
//                topHashTags.put(name,(String) tuple.getValue(1));
//            }
//        }else if(componentId.equals("my-names-spout")){
//            String name = tuple.getString(0);
//
//            if (topHashTags.containsKey(name)){
//                sentence = name+"'s favorite is "+topHashTags.get(name)+"!!!";
//
//                _collector.emit(tuple, new Values(sentence));
//            }
//        }else if (componentId.equals("exclaim1")){
//            sentence = (String) tuple.getValue(0);
//
//            _collector.emit(tuple, new Values(sentence+"!!!"));
//        }
//
//        // build the word with the exclamation marks appended
//
//
//        // emit the word with exclamations
//
//    }
//




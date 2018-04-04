package udacity.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class CountBolt extends BaseRichBolt{
    /**
     * A bolt that counts the words that it receives
     */

      // To output tuples from this bolt to the next stage bolts, if any
      private OutputCollector collector;

      // Map to store the count of the words
      private Map<String, Integer> countMap;



      @Override
      public void prepare(
              Map                     map,
              TopologyContext topologyContext,
              OutputCollector         outputCollector)
      {

        // save the collector for emitting tuples
        collector = outputCollector;

        // create and initialize the map
        countMap = new HashMap<String, Integer>();
      }

      @Override
      public void execute(Tuple tuple)
      {
        // get the word from the 1st column of incoming tuple
        String word = tuple.getString(0);

        // check if the word is present in the map
        if (countMap.get(word) == null) {

          // not present, add the word with a count of 1
          countMap.put(word, 1);
        } else {

          // already there, hence get the count
          Integer val = countMap.get(word);

          // increment the count and save it to the map
          countMap.put(word, ++val);
        }


        // emit the word and count
        collector.emit(new Values(word, Long.valueOf(countMap.get(word))));
      }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
      {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a two columns called 'word' and 'count'

        // declare the first column 'word', second column 'count'
        outputFieldsDeclarer.declare(new Fields("word","count"));
      }

      private LinkedHashMap<String, Integer> sortHashMap(HashMap<String, Integer> unsortedMap){

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
          LinkedHashMap<String, Integer> sortedHashTags = new LinkedHashMap<String, Integer>();

          for (Map.Entry<String, Integer> entry: listOfEntries){
            sortedHashTags.put(entry.getKey(),entry.getValue());
          }
          return sortedHashTags;
      }


}

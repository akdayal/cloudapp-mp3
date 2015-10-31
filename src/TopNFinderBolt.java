import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.*;
import java.util.Map.Entry;

/**
 * a bolt that finds the top n words.
 */
public class TopNFinderBolt extends BaseBasicBolt {
  private HashMap<String, Integer> currentTopWords = new HashMap<String, Integer>();
  private int N;

  private long intervalToReport = 20;
  private long lastReportTime = System.currentTimeMillis();

  public TopNFinderBolt(int N) {
    this.N = N;
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
 /*
    ----------------------TODO-----------------------
    Task: keep track of the top N words


    ------------------------------------------------- */
    String newKey = tuple.getString(0);
    Integer newVal = tuple.getInteger(1);
    // put all tuples if size is less than N
    if(currentTopWords.size() < N){
        currentTopWords.put(newKey,newVal);
    }

    // else, figure out whether exisiting keys has less than upcoming tuple
    // figure out those key and replace it with new tuple
    else {
        String repKey = null;
        // sort existign entry in map and replace those key that is just small than new key
        List<Map.Entry<String, Integer>> sortedListOfMap = sotMap(currentTopWords);

        // first entry will have minimum entry in currentTopWords
        // check whether newKey has value more than minimum's key value
        Entry<String, Integer> ent = sortedListOfMap.get(0);
        if(newVal > ent.getValue()) {
            repKey = ent.getKey();
        }
        else if(newVal == ent.getValue()){
            if(newKey.compareTo(ent.getKey()) >= 0){
                 repKey = ent.getKey();
            }
        }

        if(repKey != null){
             //remove repKey and replace it with newKey
             currentTopWords.remove(repKey);
             currentTopWords.put(newKey,newVal);
        }
    }

    //reports the top N words periodically
    if (System.currentTimeMillis() - lastReportTime >= intervalToReport) {
      collector.emit(new Values(printMap()));
      lastReportTime = System.currentTimeMillis();
    }
  }

  private List<Entry<String, Integer>> sotMap(HashMap<String, Integer> map) {
	List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
	Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

	    @Override
	    public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
		// if value are equal, use lexigraphy of key
		// have done it deliberately 
		if(o1.getValue() == o2.getValue()){
			return o2.getKey().compareTo(o1.getKey());
		}
		else{
			return (o1.getValue().compareTo(o2.getValue()));
		}
	   }
			
      });
		
     return list;
   }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

     declarer.declare(new Fields("top-N"));

  }

  public String printMap() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("top-words = [ ");
    for (String word : currentTopWords.keySet()) {
      stringBuilder.append("(" + word + " , " + currentTopWords.get(word) + ") , ");
    }
    int lastCommaIndex = stringBuilder.lastIndexOf(",");
    stringBuilder.deleteCharAt(lastCommaIndex + 1);
    stringBuilder.deleteCharAt(lastCommaIndex);
    stringBuilder.append("]");
    return stringBuilder.toString();

  }
}

package bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt extends BaseBasicBolt {
    Integer id;
    String name;
    Map<String, Integer> counters;
    String fileName;

    public void prepare(Map conf, TopologyContext context) {
        this.counters = new HashMap<String, Integer>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.fileName = conf.get("dirToWrite").toString() + "output-" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt";
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String str = input.getString(0);
        if(!counters.containsKey(str)){
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
    }

    public void cleanup() {
        try{
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");
            for (Map.Entry<String, Integer> entry: counters.entrySet()) {
                writer.println(entry.getKey() + ": " + entry.getValue());
            }
            writer.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}

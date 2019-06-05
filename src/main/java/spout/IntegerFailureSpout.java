package spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class IntegerFailureSpout extends BaseRichSpout {

    private static Integer MAX_FAILS = 3;
    private SpoutOutputCollector collector;
    private Map<Integer, Integer> integerFailureCount;
    private List<Integer> toSend;
    private static Logger LOG = Logger.getLogger(IntegerFailureSpout.class.getName());

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.toSend = new ArrayList<Integer>();
        for (int i = 0; i < 100; i++){
            toSend.add(i);
        }
        this.integerFailureCount = new HashMap<Integer, Integer>();
        this.collector = collector;
    }

    public void nextTuple() {
        if(!toSend.isEmpty()) {
            for(Integer current: toSend) {
                Integer intBucket = (current / 10);
                this.collector.emit(new Values(current.toString(), intBucket.toString()), current);
            }
        }
        toSend.clear();
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer", "bucket"));
    }

    public void ack(Object msgId) {
        System.out.println(msgId + " successful");
    }

    public void fail(Object msgId) {
        Integer failures = 1;
        Integer failId = (Integer) msgId;

        if(integerFailureCount.containsKey(failId)) {
            failures = integerFailureCount.get(failId) + 1;
        }
        if(failures < MAX_FAILS) {
            integerFailureCount.put(failId, failures);
            toSend.add(failId);
            LOG.info("Re-sending message : " + failId);
        } else {
            LOG.info("Sending Message : " + failId + " failed");
        }
    }
}

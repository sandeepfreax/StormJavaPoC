package spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class DirectGroupingSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private Integer i = 0;
    private List<Integer> boltIds;

    public void ack(Object msgId) {}

    public void fail(Object msgId) {}

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
        this.boltIds = context.getComponentTasks("Int-To-Bucket-Bolt");
    }

    public void nextTuple() {
        while (i < 100) {
            Integer intBucket = (this.i/10);
            this.spoutOutputCollector.emitDirect(boltIds.get(getBoltId(intBucket)), new Values(this.i.toString(), intBucket.toString()));
            this.i = this.i + 1;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integer", "bucket"));
    }

    private Integer getBoltId(Integer intBucket) {
        return  intBucket % boltIds.size();
    }


}

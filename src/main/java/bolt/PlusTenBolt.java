package bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PlusTenBolt extends BaseBasicBolt {

    public void cleanup() {}

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        Integer input = Integer.parseInt(tuple.getString(1));
        Integer output = input + 10;
        collector.emit(new Values(tuple.getValue(0), output.toString())); //id field will help the storm to record what tuple has been processed so far
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}

package bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FileReaderAfterBolt extends BaseBasicBolt {

    public void cleanUp() {}

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        collector.emit(new Values(tuple.getString(0) + " After Bolt"));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}

package bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;

public class TwitterStatusBolt extends BaseBasicBolt {

    public void cleanup() {}

    public void execute(Tuple input, BasicOutputCollector collector) {
        Status status = (Status) input.getValueByField("tweet");
        String tweetText = status.getText();
        collector.emit(new Values(tweetText));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }


}

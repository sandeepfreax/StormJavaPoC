package bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FilterFieldBolt extends BaseBasicBolt {

    public void cleanUp() {}

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String eventName = tuple.getStringByField("eventName"); //can be extracted by field name too
        double price = Double.parseDouble(tuple.getString(2));

        collector.emit(new Values(eventName, price));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("eventName", "eventPrice"));
    }
}

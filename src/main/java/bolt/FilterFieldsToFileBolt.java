package bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class FilterFieldsToFileBolt extends BaseBasicBolt {

    private PrintWriter writer;

    public void prepare(Map conf, TopologyContext context) {
        String fileName = "output" + context.getThisTaskId() + "-" + context.getThisComponentId() + ".txt";
        try {
            this.writer = new PrintWriter(conf.get("dirToWrite").toString() + fileName, "UTF-8");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String eventName = tuple.getStringByField("eventName"); //can be extracted by field name too
        double price = Double.parseDouble(tuple.getString(2));

        writer.println(eventName + "," + price);
        collector.emit(new Values(eventName, price));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("eventName", "eventPrice"));
    }

    public void cleanup() {
        writer.close();
    }
}

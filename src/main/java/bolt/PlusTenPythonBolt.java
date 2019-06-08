package bolt;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class PlusTenPythonBolt extends ShellBolt implements IRichBolt {

    public PlusTenPythonBolt() {
        super("python", "D:\\sandeep\\workspaceInteliJ\\stormPoc\\StormJavaPoC\\src\\main\\multilang\\resources\\plusTenBolt.py");
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("number"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

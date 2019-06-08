package spout;

import org.apache.storm.spout.ShellSpout;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class IntegerSpoutPython extends ShellSpout implements IRichSpout {

    public IntegerSpoutPython() {
        super("python",
                "D:\\sandeep\\workspaceInteliJ\\stormPoc\\StormJavaPoC\\src\\main\\multilang\\resources\\integerSpout.py");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("integer","bucket"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}

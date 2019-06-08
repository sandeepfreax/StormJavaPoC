package tridentFunction;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SimpleTridentFunction extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        collector.emit(new Values(tuple.getString(0) + " after processing"));
    }
}

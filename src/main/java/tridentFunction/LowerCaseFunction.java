package tridentFunction;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class LowerCaseFunction implements MapFunction {

    public Values execute(TridentTuple tuple) {
        return new Values(tuple.getString(0).toLowerCase());
    }
}

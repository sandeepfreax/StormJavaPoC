package tridentFunction;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class ErrorAggregator extends BaseAggregator<ErrorAggregator.State> {

    public ErrorAggregator() {    }

    public ErrorAggregator.State init(Object batchId, TridentCollector collector) {
        return new ErrorAggregator.State();
    }

    public void aggregate(ErrorAggregator.State state, TridentTuple tuple, TridentCollector collector) {
        if(tuple.getString(0).equals("ERROR"))
            ++state.count;
    }

    public void complete(ErrorAggregator.State state, TridentCollector collector) {
        collector.emit(new Values(new Object[]{Long.valueOf(state.count)}));
    }

    static class State {
        long count = 0L;
        State() {
        }
    }

}

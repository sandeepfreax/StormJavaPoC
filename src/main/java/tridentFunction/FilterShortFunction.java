package tridentFunction;

import org.apache.storm.trident.tuple.TridentTuple;
import org.glassfish.grizzly.filterchain.BaseFilter;

public class FilterShortFunction extends BaseFilter {

    public boolean isKeep(TridentTuple tuple) {
        return tuple.getString(0).length()>3;
    }
}

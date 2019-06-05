package topology;

import clojure.lang.Obj;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MyCustomGrouping implements CustomStreamGrouping, Serializable {

    private List<Integer> targetTasks;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        this.targetTasks = targetTasks;
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        List<Integer> boltIds = new ArrayList<Integer>();
        Integer boltNum = Integer.parseInt(values.get(1).toString()) % targetTasks.size();
        boltIds.add(targetTasks.get(boltNum));
        return boltIds;
    }
}

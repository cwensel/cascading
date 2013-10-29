package cascading.flow.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import cascading.flow.Flow;
import cascading.flow.FlowStep;
import cascading.flow.FlowStepStrategy;
import cascading.tap.Tap;
import cascading.tap.hadoop.ReducerEstimater;

public class ReducerEstimaterStrategy implements FlowStepStrategy<JobConf> {

	@Override
	public void apply(Flow<JobConf> flow,
			List<FlowStep<JobConf>> predecessorSteps, FlowStep<JobConf> flowStep) {
		Collection<Tap> taps = flow.getSourcesCollection();
		JobConf conf = flowStep.getConfig();
		boolean allEstimatable = true;
		for (Tap tap : taps) {
			if (!(tap instanceof ReducerEstimater)) {
				allEstimatable = false;
			}
		}
		if (allEstimatable) {
			int reducerNum = 0;
			for (Tap tap : taps) {
				try {
					reducerNum += ((ReducerEstimater) tap).getReducerNum(conf);
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException(
							"IOException happens when estimating reducer from tap:"
									+ tap.getIdentifier());
				}
			}
			conf.setNumReduceTasks(reducerNum);
		}
	}

}

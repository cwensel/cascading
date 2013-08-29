package cascading.tap.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;

public interface ReducerEstimater {

        public int getReducerNum(JobConf conf) throws IOException;
}

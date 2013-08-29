package cascading.tap.hadoop.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerEstimaterUtil {

	private static final Logger LOG = LoggerFactory
			.getLogger(ReducerEstimaterUtil.class);

	private static final String BYTES_PER_REDUCER_PARAM = "cascading.exec.reducers.bytes.per.reducer";
	private static final String MAX_REDUCER_COUNT_PARAM = "cascading.exec.reducers.max";

	private static final long DEFAULT_BYTES_PER_REDUCER = 1000 * 1000 * 1000;
	private static final int DEFAULT_MAX_REDUCER_COUNT_PARAM = 999;

	public static int estimateFromPath(List<Path> paths, JobConf conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		long bytesPerReducer = conf.getLong(BYTES_PER_REDUCER_PARAM,
				DEFAULT_BYTES_PER_REDUCER);
		int maxReducers = conf.getInt(MAX_REDUCER_COUNT_PARAM,
				DEFAULT_MAX_REDUCER_COUNT_PARAM);

		long totalInputFileSize = getTotalInputFileSize(paths, fs);

		LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
				+ maxReducers + " totalInputFileSize=" + totalInputFileSize);

		int reducers = (int) Math.ceil((double) totalInputFileSize
				/ bytesPerReducer);
		reducers = Math.max(1, reducers);
		reducers = Math.min(maxReducers, reducers);

		return reducers;
	}

	private static long getTotalInputFileSize(List<Path> paths, FileSystem fs)
			throws IOException {
		long totalInputFileSize = 0;
		for (Path path : paths) {
			FileStatus[] statuses = fs.listStatus(path);
			for (FileStatus status : statuses) {
				totalInputFileSize += status.getLen();
			}
		}
		return totalInputFileSize;
	}
}
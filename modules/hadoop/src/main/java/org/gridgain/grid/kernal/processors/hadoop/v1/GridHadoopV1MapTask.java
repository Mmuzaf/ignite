/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v1;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Hadoop map task implementation for v1 API.
 */
public class GridHadoopV1MapTask extends GridHadoopTask {
    /** */
    private static final String[] EMPTY_HOSTS = new String[0];

    /** {@inheritDoc} */
    public GridHadoopV1MapTask(GridHadoopTaskInfo taskInfo) {
        super(taskInfo);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void run(final GridHadoopTaskContext taskCtx) throws GridException {
        GridHadoopV2Job jobImpl = (GridHadoopV2Job) taskCtx.job();

        JobConf jobConf = new JobConf(jobImpl.hadoopJobContext().getJobConf());

        Mapper mapper = U.newInstance(jobConf.getMapperClass());

        InputFormat inFormat = jobConf.getInputFormat();

        GridHadoopInputSplit split = info().inputSplit();

        InputSplit nativeSplit;

        if (split instanceof GridHadoopFileBlock) {
            GridHadoopFileBlock block = (GridHadoopFileBlock)split;

            nativeSplit = new FileSplit(new Path(block.file().toString()), block.start(), block.length(), EMPTY_HOSTS);
        }
        else if (split instanceof GridHadoopExternalSplit)
            nativeSplit = jobImpl.readExternalSplit((GridHadoopExternalSplit)split);
        else {
            assert split != null;

            nativeSplit = split.innerSplit();
        }

        assert nativeSplit != null;

        Reporter reporter = Reporter.NULL;

        try {
            GridHadoopOutputCollector collector = new GridHadoopOutputCollector(jobConf, taskCtx,
                !jobImpl.hasCombinerOrReducer(), fileName(), jobImpl.attemptId(info()));

            RecordReader reader = inFormat.getRecordReader(nativeSplit, jobConf, reporter);

            Object key = reader.createKey();
            Object val = reader.createValue();

            assert mapper != null;

            mapper.configure(jobConf);

            try {
                while (reader.next(key, val))
                    mapper.map(key, val, collector, reporter);

                mapper.close();
            }
            finally {
                collector.close();
            }
        }
        catch (IOException e) {
            throw new GridException(e);
        }
    }
}

package no.uni.computing.lib.input.extend;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.List;

@Public
@Stable
public class SequenceFileInputFormatCustom extends FileInputFormat{
    public SequenceFileInputFormatCustom() {

    }

    public RecordReader<Text, ByteWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader<Text, ByteWritable>();
    }

    /*Not Help, the getFormatMinSplitSize Help
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }*/

    protected long getFormatMinSplitSize() {
        return 9223372036854775807L;
    }

    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List files = super.listStatus(job);
        int len = files.size();

        for (int i = 0; i < len; ++i) {
            FileStatus file = (FileStatus) files.get(i);
            if (file.isDirectory()) {
                Path p = file.getPath();
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                files.set(i, fs.getFileStatus(new Path(p, "data")));
            }
        }

        return files;
    }

}

package reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetPropertiesAndClassesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
  private NullWritable nothing = NullWritable.get();
  @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write (key, nothing);

    }
}

package mappers;

import java.io.IOException;

import data.Triple;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/* ================================================ */
public class GetAllPredicateClassesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  private Text oKey   = new Text();
  private IntWritable oValue = new IntWritable();
  /* ----------------------------------- */
  @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      Triple inputTriple = null;
      /* -------------------------- */
      try {
        inputTriple = new Triple(value.toString());
      } catch (Exception e) {
        /* ignore */
        return;
      }
      /* ----------------------------------- */
      oKey.set (inputTriple.getPredicate());
      oValue.set (1);
      context.write (oKey, oValue);
      /* ----------------------------------- */
      oKey.set (inputTriple.getObject());
      oValue.set (2);
      context.write (oKey, oValue);
    }
}

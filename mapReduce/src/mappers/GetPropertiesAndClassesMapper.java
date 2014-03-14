package mappers;

import java.io.IOException;

import data.Triple;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* ================================================ */
public class GetPropertiesAndClassesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private NullWritable nothing = NullWritable.get();
  private Text oKey = new Text();
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
      if (inputTriple.getPredicate().matches(".*#type>")) {
        if (inputTriple.getObject().matches(".*#Class>")) {
          oKey.set (inputTriple.getSubject() + " Class");   // We are only interested in predicates
          context.write (oKey, nothing);
        } else if (inputTriple.getObject().matches(".*#Property>")) {
          oKey.set (inputTriple.getSubject() + " Property");   // We are only interested in predicates
          context.write (oKey, nothing);
        }
      }
    }
}

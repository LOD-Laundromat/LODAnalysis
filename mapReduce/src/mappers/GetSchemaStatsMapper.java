package mappers;

import java.io.IOException;

import data.Triple;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* ================================================ */
public class GetSchemaStatsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private NullWritable nothing = NullWritable.get();
  private Text oKey = new Text();
  /* ----------------------------------- */
  private boolean isSchema (String term) {
    return term.matches ("^.*http://www.w3.org/.*/(rdf-syntax-ns|rdf-schema|owl|XMLSchema)#.*$");
  }
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
      if (isSchema(inputTriple.getSubject())) {
        oKey.set (inputTriple.getSubject());
      } else if (isSchema(inputTriple.getPredicate())) {
        oKey.set (inputTriple.getPredicate());
      } else if (isSchema(inputTriple.getObject())) {
        oKey.set (inputTriple.getObject());
      }
      context.write (oKey, nothing);
    }
}

package lodanalysis.model.statements;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import lodanalysis.Paths;
import lodanalysis.model.CreateModel;
import lodanalysis.model.CreateModelStatement;
import lodanalysis.model.CreateModel.Namespace;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;


public class NumDistinctRDFNodes extends CreateModelStatement {


	public NumDistinctRDFNodes(CreateModel factory) {
		super(factory);
	}

	@Override
	public void createDescription() throws IOException {
	    int bnodes = 0;
	    try {
	        bnodes = countLines(new File(dir, Paths.BNODE_COUNTS));
	    } catch (Exception IOException) {
	        //just skip
	    }
	    int literals = 0;
	    try {
	        literals = Integer.parseInt(FileUtils.readFileToString(new File(dir, Paths.DISTINCT_LITERALS)));
	    } catch(Exception e) {
	        //just skip
	    }
	    int uris = 0;
	    try {
            uris = Integer.parseInt(FileUtils.readFileToString(new File(dir, Paths.DISTINCT_URIS)));
        } catch(Exception e) {
            //just skip
        }
	    
		doc.addProperty(getProp(Namespace.LLM, "distinctRDFNodes"), Integer.toString(bnodes + literals + uris), XSDDatatype.XSDlong);
	}

}

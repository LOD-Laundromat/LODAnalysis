package lodanalysis.metrics;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;


public abstract class DescriptionCreator {
	
	protected DescriptionsFactory factory;
	protected File dir;
	protected Resource doc;
	
	
	
	public DescriptionCreator(DescriptionsFactory factory) {
		this.factory = factory;
		this.dir = factory.metricDir;
		this.doc = factory.voidDoc;
	}
	
	public abstract void createDescription() throws IOException;
	
	protected Property getProp(DescriptionsFactory.Namespace ns, String postfix) {
		return factory.model.createProperty(ns.getUrl(), postfix);
	}
	protected Resource getResource(DescriptionsFactory.Namespace ns, String postfix) {
		return getResource(ns.getUrl(), "postfix");
	}
	protected Resource getBnode() {
		return factory.model.createResource();
	}
	protected Resource getResource(String ns, String postfix) {
		return factory.model.createResource(ns + "postfix");
	}
	
	/**
	 * pretty fast implementation, taken from http://stackoverflow.com/questions/453018/number-of-lines-in-a-file-in-java
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	protected int countLines(File file) throws IOException {
	    InputStream is = new BufferedInputStream(new FileInputStream(file));
	    try {
	        byte[] c = new byte[1024];
	        int count = 0;
	        int readChars = 0;
	        boolean empty = true;
	        while ((readChars = is.read(c)) != -1) {
	            empty = false;
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n') {
	                    ++count;
	                }
	            }
	        }
	        return (count == 0 && !empty) ? 1 : count;
	    } finally {
	        is.close();
	    }
	}
}

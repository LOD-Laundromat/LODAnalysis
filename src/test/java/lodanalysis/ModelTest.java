package lodanalysis;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.Assert;
import lodanalysis.streamer.Position;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class ModelTest {
	private static String metricsDir = "src/test/resources/re/sourcestestDataset";
	@BeforeClass
	public static void testSetup() throws IOException {
	     new Entry(new String[]{"-force","-dataset", "src/test/resources/testDataset", "-metrics", "src/test/resources/", "lodanalysis.streamer.StreamDatasets", });
//	     new Entry(new String[]{"-force", "-metric", metricsDir, "lodanalysis.model.CreateModels", });
	}

	@AfterClass
	public static void testCleanup() throws IOException {
	    FileUtils.deleteDirectory(new File(metricsDir));
	}

	@Test
	public void testNumIris() throws IOException {
	  //more of a regression test, but so be it
	    Assert.assertEquals("3.25", FileUtils.readFileToString(new File(metricsDir, "degreeAvg")));//check
	    Assert.assertEquals("7.0", FileUtils.readFileToString(new File(metricsDir, "degreeMax")));//check
	    Assert.assertEquals("2", FileUtils.readFileToString(new File(metricsDir, "distinctBnodesObj")));//check
	    Assert.assertEquals("2", FileUtils.readFileToString(new File(metricsDir, "distinctBnodesSub")));//check
	    Assert.assertEquals("1", FileUtils.readFileToString(new File(metricsDir, "distinctDataTypes")));//check
	    Assert.assertEquals("3", FileUtils.readFileToString(new File(metricsDir, "distinctLiteralCount")));//check
	    Assert.assertEquals("15", FileUtils.readFileToString(new File(metricsDir, "distinctObjs")));//check
	    Assert.assertEquals("8", FileUtils.readFileToString(new File(metricsDir, "distinctSubjectCount")));//check
	    Assert.assertEquals("18", FileUtils.readFileToString(new File(metricsDir, "distinctUris")));//check
	    Assert.assertEquals("10", FileUtils.readFileToString(new File(metricsDir, "distinctUrisObj")));//check
	    Assert.assertEquals("1.5", FileUtils.readFileToString(new File(metricsDir, "indegreeAvg")));//check
	    Assert.assertEquals("1.0", FileUtils.readFileToString(new File(metricsDir, "indegreeMedian")));//check
	    Assert.assertEquals("1.0", FileUtils.readFileToString(new File(metricsDir, "indegreeMin")));//check
	    
	    Assert.assertEquals("3", FileUtils.readFileToString(new File(metricsDir, "numLiterals")));//check
	    Assert.assertEquals("52", FileUtils.readFileToString(new File(metricsDir, "numUris")));//check
	    Assert.assertEquals("2.625", FileUtils.readFileToString(new File(metricsDir, "outdegreeAvg")));//check
	    Assert.assertEquals("2.5", FileUtils.readFileToString(new File(metricsDir, "outdegreeMedian")));//check
	    Assert.assertEquals("21", FileUtils.readFileToString(new File(metricsDir, "tripleCount")));//check
	    Assert.assertEquals("33.75", FileUtils.readFileToString(new File(metricsDir, "uriLengthAvg")));//check
	    Assert.assertEquals("32.0", FileUtils.readFileToString(new File(metricsDir, "uriLengthMedian")));//check
	    Assert.assertEquals("30.5", FileUtils.readFileToString(new File(metricsDir, "uriObjLengthMedian")));//check
	    Assert.assertEquals("26.0", FileUtils.readFileToString(new File(metricsDir, "uriObjLengthMin")));//check
	    Assert.assertEquals("40.0", FileUtils.readFileToString(new File(metricsDir, "uriPredLengthMedian")));//check
	    Assert.assertEquals("30.6", FileUtils.readFileToString(new File(metricsDir, "uriSubLengthAvg")));//check
	    
	    Assert.assertEquals("2.0", FileUtils.readFileToString(new File(metricsDir, "literalLengthMax")));//checked (fixed!)
	    Assert.assertEquals("1.0", FileUtils.readFileToString(new File(metricsDir, "literalLengthMedian")));//checked (fixed!)
	    
	    
	    //line counts
	    Assert.assertEquals(18, FileUtils.readFileToString(new File(metricsDir, "namespaceCounts")).split("\\n").length);//check
	    Assert.assertEquals(8, FileUtils.readFileToString(new File(metricsDir, "predicateCounts")).split("\\n").length);//check
	    Assert.assertEquals(4, FileUtils.readFileToString(new File(metricsDir, "typeCounts")).split("\\n").length);//check
	    

	}
	
}
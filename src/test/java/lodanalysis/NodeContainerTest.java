package lodanalysis;

import static org.junit.Assert.assertEquals;
import lodanalysis.utils.NodeContainer;
import lodanalysis.utils.NodeContainer.Position;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeContainerTest {
	
	@BeforeClass
	public static void testSetup() {
	}

	@AfterClass
	public static void testCleanup() {
		// Teardown for data used by the unit tests
	}

	@Test
	public void testNodes() {
		runCase(
				"<http://google.com_1>", 
				Position.OBJ, 
				"http://google.com_1", //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
			);
		runCase(
				"<http://google.co/df/fdm_1111>", 
				Position.OBJ, 
				"http://google.co/df", //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"<http://google.co/df#fdm_>", 
				Position.OBJ, 
				"http://google.co/df", //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"<http://google.co/df#_12332>", 
				Position.PRED, 
				"http://google.co/df", //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"<http://google.co/df/_12332>", 
				Position.SUB, 
				"http://google.co/df", //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_111>", 
				Position.SUB, 
				null, //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				true //ignoreUri
				);
		runCase(
				"<http://www.w3.org/1999/02/22-rdf-syntax-ns#_>", 
				Position.SUB, 
				"http://www.w3.org/1999/02/22-rdf-syntax-ns", //ns
				null, //datatype
				false,//isliteral 
				true, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"\"That Seventies Show\"^^<http://www.w3.org/2001/XMLSchema#string>", 
				Position.OBJ, 
				null, //ns
				"http://www.w3.org/2001/XMLSchema#string", //datatype
				true,//isliteral 
				false, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"\"That Seventies Show\"", 
				Position.OBJ, 
				null, //ns
				null, //datatype
				true,//isliteral 
				false, //isUri
				false, //isBnode
				null, //langtag
				null, //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"\"That Seventies Show\"@en", 
				Position.OBJ, 
				null, //ns
				null, //datatype
				true,//isliteral 
				false, //isUri
				false, //isBnode
				"en", //langtag
				"en", //langtagwithoutregion
				false //ignoreUri
				);
		runCase(
				"\"That Seventies Show\"@en-be", 
				Position.OBJ, 
				null, //ns
				null, //datatype
				true,//isliteral 
				false, //isUri
				false, //isBnode
				"en-be", //langtag
				"en", //langtagwithoutregion
				false //ignoreUri
				);
		
		
	}
	
	public static void runCase(String origNode, Position position, String ns, String datatype, Boolean isLiteral, Boolean isUri, Boolean isBnode, String langTag, String langTagWithoutReg, Boolean ignoreIri) {
		NodeContainer node = new NodeContainer(origNode, position);
		
		 assertEquals(origNode + ": origNode does not match", origNode, node.stringRepresentation);
		 assertEquals(origNode + ": datatype does not match", datatype, node.datatype);
		 assertEquals(origNode + ": isLiteral does not match", isLiteral, node.isLiteral);
		 assertEquals(origNode + ": isUri does not match", isUri, node.isUri);
		 assertEquals(origNode + ": isBnode does not match", isBnode, node.isBnode);
		 assertEquals(origNode + ": langTag does not match", langTag, node.langTag);
		 assertEquals(origNode + ": langTagWithoutReg does not match", langTagWithoutReg, node.langTagWithoutReg);
		 assertEquals(origNode + ": ignoreIri does not match", ignoreIri, node.ignoreIri);
		 assertEquals(origNode + ": ns does not match", ns, node.ns);
	}
}
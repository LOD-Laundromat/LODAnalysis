package lodanalysis;

import static org.junit.Assert.assertArrayEquals;
import lodanalysis.aggregator.AggregateDataset;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NtReaderTest {
	
	@BeforeClass
	public static void testSetup() {
	}

	@AfterClass
	public static void testCleanup() {
		// Teardown for data used by the unit tests
	}

	@Test
	public void testNodes() {
		assertArrayEquals(
			new String[]{
					"http://atlantides.org/capgrids/99#this-extent",
					"http://data.ordnancesurvey.co.uk/ontology/geometry/asWKT",
					"\"POLYGON ((71.0000000000000000 35.0000000000000000, 71.0000000000000000 39.0000000000000000, 66.0000000000000000 39.0000000000000000, 66.0000000000000000 35.0000000000000000, 71.0000000000000000 35.0000000000000000))\"",
			},
			AggregateDataset.getNodes("<http://atlantides.org/capgrids/99#this-extent> <http://data.ordnancesurvey.co.uk/ontology/geometry/asWKT> \"POLYGON ((71.0000000000000000 35.0000000000000000, 71.0000000000000000 39.0000000000000000, 66.0000000000000000 39.0000000000000000, 66.0000000000000000 35.0000000000000000, 71.0000000000000000 35.0000000000000000))\" .")
		);
		assertArrayEquals(
				new String[]{
						"http://blaat1",
						"http://blaa2",
						"\"sdf\"@en-be",
				},
				AggregateDataset.getNodes("<http://blaat1> <http://blaa2> \"sdf\"@en-be .")
		);
		assertArrayEquals(
				new String[]{
						"http://blaat1",
						"http://blaa2",
						"\"sdf\"^^<http://stringggg>",
				},
				AggregateDataset.getNodes("<http://blaat1> <http://blaa2> \"sdf\"^^<http://stringggg> .")
		);
		assertArrayEquals(
				new String[]{
						"http://blaat1",
						"http://blaa2",
						"http://blaa3",
				},
				AggregateDataset.getNodes("<http://blaat1> <http://blaa2> <http://blaa3> .")
		);
		

	}
}
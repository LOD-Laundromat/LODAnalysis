package lodanalysis;

import static org.junit.Assert.assertArrayEquals;
import lodanalysis.streamer.StreamDataset;

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
					null
			},
			StreamDataset.parseStatement("<http://atlantides.org/capgrids/99#this-extent> <http://data.ordnancesurvey.co.uk/ontology/geometry/asWKT> \"POLYGON ((71.0000000000000000 35.0000000000000000, 71.0000000000000000 39.0000000000000000, 66.0000000000000000 39.0000000000000000, 66.0000000000000000 35.0000000000000000, 71.0000000000000000 35.0000000000000000))\" .", false)
		);
		assertArrayEquals(
		        new String[]{
		                "http://atlantides.org/capgrids/99#this-extent",
		                "http://data.ordnancesurvey.co.uk/ontology/geometry/asWKT",
		                "\"POLYGON ((71.0000000000000000 35.0000000000000000, 71.0000000000000000 39.0000000000000000, 66.0000000000000000 39.0000000000000000, 66.0000000000000000 35.0000000000000000, 71.0000000000000000 35.0000000000000000))\"",
		                null
		        },
		        StreamDataset.parseStatement("<http://atlantides.org/capgrids/99#this-extent> <http://data.ordnancesurvey.co.uk/ontology/geometry/asWKT> \"POLYGON ((71.0000000000000000 35.0000000000000000, 71.0000000000000000 39.0000000000000000, 66.0000000000000000 39.0000000000000000, 66.0000000000000000 35.0000000000000000, 71.0000000000000000 35.0000000000000000))\" .", true)
		        );
		assertArrayEquals(
				new String[]{
						"http://blaat1",
						"http://blaa2",
						"\"sdf\"@en-be",
						null
				},
				StreamDataset.parseStatement("<http://blaat1> <http://blaa2> \"sdf\"@en-be .", false)
		);
		assertArrayEquals(
		        new String[]{
		                "http://blaat1",
		                "http://blaa2",
		                "\"sdf\"@en-be",
		                null
		        },
		        StreamDataset.parseStatement("<http://blaat1> <http://blaa2> \"sdf\"@en-be .", true)
		        );
		assertArrayEquals(
				new String[]{
						"http://blaat1",
						"http://blaa2",
						"\"sdf\"^^<http://stringggg>",
						null
				},
				StreamDataset.parseStatement("<http://blaat1> <http://blaa2> \"sdf\"^^<http://stringggg> .", false)
		);
		assertArrayEquals(
		        new String[]{
		                "http://blaat1",
		                "http://blaa2",
		                "\"s df\"^^<http://stringggg>",
		                null
		        },
		        StreamDataset.parseStatement("<http://blaat1> <http://blaa2> \"s df\"^^<http://stringggg> .", true)
		        );
		assertArrayEquals(
				new String[]{
						"http://blaat1",
						"http://blaa2",
						"http://blaa3",
						null
				},
				StreamDataset.parseStatement("<http://blaat1> <http://blaa2> <http://blaa3> .", false)
		);
		assertArrayEquals(
		        new String[]{
		                "http://blaat1",
		                "http://blaa2",
		                "http://blaa3",
		                null
		        },
		        StreamDataset.parseStatement("<http://blaat1> <http://blaa2> <http://blaa3> .", true)
		        );
		assertArrayEquals(
		        new String[]{
		                "http://dbpedia.org/data/M4_(computer_language).xml",
		                "http://code.google.com/p/ldspider/ns#headerInfo",
		                "http://lodlaundromat.org/.well-known/genid/0f",
		                "http://dbpedia.org/data/M4_(computer_language).xml",
		        },
		        StreamDataset.parseStatement("<http://dbpedia.org/data/M4_(computer_language).xml> <http://code.google.com/p/ldspider/ns#headerInfo> <http://lodlaundromat.org/.well-known/genid/0f> <http://dbpedia.org/data/M4_(computer_language).xml> .", true)
        );
		assertArrayEquals(
		        new String[]{
		                "http://dbpedia.org/resource/0A",
		                "http://dbpedia.org/ontology/abstract",
		                "\"0A (zeress than 0.1 ppm total hydrocarbons.\"@en",
		                "http://dbpedia.org/data/0A.xml"
		        },
		        StreamDataset.parseStatement("<http://dbpedia.org/resource/0A> <http://dbpedia.org/ontology/abstract> \"0A (zeress than 0.1 ppm total hydrocarbons.\"@en <http://dbpedia.org/data/0A.xml> .", true)
        );

	}
}
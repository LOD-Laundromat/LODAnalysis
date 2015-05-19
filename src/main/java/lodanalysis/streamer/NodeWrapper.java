package lodanalysis.streamer;

public class NodeWrapper {
    
    /**
     * Statics
     */
	public static final String BNODE_SUBSTRING = "http://lodlaundromat.org/.well-known/";
	private final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private static final String RDF_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class";
	private static final String OWL_CLASS = "http://www.w3.org/2002/07/owl#Class";
	private static final String RDF_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	private static final String OWL_OBJECT_PROPERTY = "http://www.w3.org/2002/07/owl#ObjectProperty";
	private static final String OWL_DATATYPE_PROPERTY = "http://www.w3.org/2002/07/owl#DatatypeProperty";
	
	
	/**
	 * persistent settings
	 * 
	 */
	
    int subCount = 0;
    int predCount = 0;
    int objCount = 0;
    int asTypeCount = 0;//e.g. <something> rdf:type <this>
    Type type = null;
    ClassType classType = null;//equals rdf:class or owl:class or rdf:property
    boolean definedAsProperty = false;//e.g. <this> rdf:type rdf:Class
    boolean definedAsClass = false;
    boolean isRdfType = false;//equals to rdf:type

    
	/**
	 * variables (MAKE SURE TO RESET THESE VALUES IN THE RESET FUNCTION)
	 * Resetting is required to avoid creating this object every time for every node in this graph
	 */
//	private Position position = null;
//	private Boolean hasW3cUriPrefix = null;
//	public PatriciaNode nsTicket = null;
//	public PatriciaNode datatype = null;
//	public boolean isLiteral = false;
//	public boolean isUri = true;
//	public boolean isBnode = false;
//	public PatriciaNode langTag = null;
//	public PatriciaNode ticket = null;
//	public int uriLength = -1;
//	public int literalLength = -1;
//	private String stringRepresentation;
//	public boolean isRdf_type = false;
//	private int dataTypeLength = 0;
//	private int langTagLength = 0;

	public NodeWrapper(String stringRepresentation) {
        calcInfo(stringRepresentation);
	}
	
	public void update(Position position) {
	    if (position == Position.SUB) {
	        subCount++;
	    } else if (position == Position.PRED) {
	        predCount++;
	    } else {
	        objCount++;
	    }
	    
	}
	
//	public void reset() {
//	    position = null;
//	    hasW3cUriPrefix = null;
//	    nsTicket = null;
//	    datatype = null;
//	    isLiteral = false;
//	    isUri = true; 
//	    isBnode = false;
//	    langTag = null;
//	    ticket = null;
//	    uriLength = -1;
//	    literalLength = -1;
//	    stringRepresentation = null;
//	    isRdf_type = false;
//	    dataTypeLength = 0;
//	    langTagLength = 0;
//	}

	
	/**
	 * do this once a-priori, as our counters often re-use info
	 * NOTE: we can afford to optimize these calculations (i.e. avoid regex), because we know how exactly we serialize the ntriples.
	 * This makes detection of things like lang tags and datatypes very easy and very fast.
	 * @param stringRepresentation 
	 */
	private void calcInfo(String stringRepresentation) {
		if (stringRepresentation.length() > 0 && stringRepresentation.charAt(0) == '"') {
			this.type = Type.LITERAL;
		} else if (stringRepresentation.startsWith(BNODE_SUBSTRING)) {
			//we rewrite each bnode to uri. check whether this is one of these uris
		    this.type = Type.BNODE;
		} else {
		    this.type = Type.URI;
//				uriLength = stringRepresentation.length();
//				String ns = getNs(stringRepresentation);
//				this.nsTicket = vault.store(ns);
		    
		    
		    
		    if (stringRepresentation.equals(RDF_CLASS) || stringRepresentation.equals(OWL_CLASS)) {
		        classType = ClassType.CLASS;
		    } else if (stringRepresentation.equals(RDF_PROPERTY) || stringRepresentation.equals(OWL_DATATYPE_PROPERTY) || stringRepresentation.equals(OWL_OBJECT_PROPERTY)) {
		        classType = ClassType.PROPERTY;
		    }
		    if (stringRepresentation.equals(RDF_TYPE)) isRdfType = true;
		}
			
	}

//	private void getLiteralLength() {
//		this.literalLength = stringRepresentation.length() - 2; //subtract the two quotes
//		
//		if (dataTypeLength > 0) {
//			//subtract datatype length, plus the two ^^
//			this.literalLength -= dataTypeLength - 2;
//		} else if (langTagLength > 0) {
//			//also subtract the @
//			this.literalLength -= langTagLength - 1;
//		}
//		
//	}

	

    

 	public int getNumOccurances() {
 	    return subCount + predCount + objCount;
 	}
	



	public static void main(String[] args) {
	    /**
	     * check namespaces
	     */
//	    System.out.println(getNs("test"));
//	    System.out.println(getNs("http://google"));
//	    System.out.println(getNs("http://google/"));
//	    System.out.println(getNs("http://google/test"));
//	    System.out.println(getNs("http://google/test?sdgds"));
//	    System.out.println(getNs("http://google/test#sdgds"));
//	    System.out.println(getNs("http://google/test:sdgds")); 
//	    System.out.println(getNs("http://dbpedia.org/resource/%22Crocodile%22_Dundee")); 
        new NodeWrapper("\"That Seventies Show\"^^<http://www.w3.org/2001/XMLSchema#string>");
//        System.out.println(vault.redeem(wrapper.datatype));
//        System.out.println(vault.redeem(wrapper.langTag));
        new NodeWrapper("\"That Seventies Show\"");
//        System.out.println(vault.redeem(wrapper.datatype));
//        System.out.println(vault.redeem(wrapper.langTag));
        new NodeWrapper("\"That Seventies Show\"@en");
//        System.out.println(vault.redeem(wrapper.datatype));
//        System.out.println(vault.redeem(wrapper.langTag));
        new NodeWrapper("\"That Seventies Show\"@en-be");
//        System.out.println(vault.redeem(wrapper.datatype));
//        System.out.println(vault.redeem(wrapper.langTag));
	    
	}
}

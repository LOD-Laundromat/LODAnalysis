package lodanalysis.streamer;

import org.apache.xerces.util.XMLChar;
import org.data2semantics.vault.PatriciaVault.PatriciaNode;
import org.data2semantics.vault.PatriciaVault;
import org.data2semantics.vault.Vault;
public class NodeWrapper {
    
    /**
     * Statics
     */
	public static final String BNODE_SUBSTRING = "http://lodlaundromat.org/.well-known/";
	private final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private static final String W3C_URI_PREFIX = "http://www.w3.org/";
	private static final String RDF_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class";
	private static final String OWL_CLASS = "http://www.w3.org/2002/07/owl#Class";
	private static final String RDF_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
	private static final String OWL_OBJECT_PROPERTY = "http://www.w3.org/2002/07/owl#ObjectProperty";
	private static final String OWL_DATATYPE_PROPERTY = "http://www.w3.org/2002/07/owl#DatatypeProperty";
	public enum Position {SUB, PRED, OBJ};
	
	
	
	private Vault<String, PatriciaNode> vault;
	
	/**
	 * variables (MAKE SURE TO RESET THESE VALUES IN THE RESET FUNCTION)
	 * Resetting is required to avoid creating this object every time for every node in this graph
	 */
	private Position position = null;
	private Boolean hasW3cUriPrefix = null;
	public PatriciaNode nsTicket = null;
	public PatriciaNode datatype = null;
	public boolean isLiteral = false;
	public boolean isUri = true;
	public boolean isBnode = false;
	public PatriciaNode langTag = null;
	public boolean ignoreIri = false;
	public PatriciaNode ticket = null;
	public int uriLength = -1;
	public int literalLength = -1;
	private String stringRepresentation;
	public boolean isRdf_type = false;
	private int dataTypeLength = 0;
	private int langTagLength = 0;

	public NodeWrapper(Vault<String, PatriciaNode> vault) {
	    this.vault = vault;
	}
	
	
	public void reset() {
	    position = null;
	    hasW3cUriPrefix = null;
	    nsTicket = null;
	    datatype = null;
	    isLiteral = false;
	    isUri = true; 
	    isBnode = false;
	    langTag = null;
	    ignoreIri = false;
	    ticket = null;
	    uriLength = -1;
	    literalLength = -1;
	    stringRepresentation = null;
	    isRdf_type = false;
	    dataTypeLength = 0;
	    langTagLength = 0;
	}

    public void init(String stringRepresentation, Position position) {
        reset();
        this.stringRepresentation = stringRepresentation;
        this.position = position;
        this.ticket = vault.store(stringRepresentation);
        calcInfo();
    }
	
	/**
	 * do this once a-priori, as our counters often re-use info
	 * NOTE: we can afford to optimize these calculations (i.e. avoid regex), because we know how exactly we serialize the ntriples.
	 * This makes detection of things like lang tags and datatypes very easy and very fast.
	 */
	private void calcInfo() {
		if (stringRepresentation.charAt(0) == '"') {
			this.isLiteral = true;
			this.isUri = false;
		}
		
		if (isUri) {
			if (stringRepresentation.startsWith(BNODE_SUBSTRING)) {
				//we rewrite each bnode to uri. check whether this is one of these uris
				this.isBnode = true;
				this.isUri = false;
			} else {
				uriLength = stringRepresentation.length();
				String ns = getNs(stringRepresentation);
				this.nsTicket = vault.store(ns);
				getSchemaInfo(stringRepresentation, ns);
			}
			
		}
		
		
		

		if (position == Position.OBJ && isLiteral) {
			
			//only for literals
			getDataType();
			getLangTagInfo();
			getLiteralLength();
			
		}

	}

	private void getLiteralLength() {
		this.literalLength = stringRepresentation.length() - 2; //subtract the two quotes
		
		if (dataTypeLength > 0) {
			//subtract datatype length, plus the two ^^
			this.literalLength -= dataTypeLength - 2;
		} else if (langTagLength > 0) {
			//also subtract the @
			this.literalLength -= langTagLength - 1;
		}
		
	}

	private void getSchemaInfo(String stringRepresentation, String ns) {
		if (position == Position.PRED && stringRepresentation.equals(RDF_TYPE)) {
			isRdf_type = true;
		}
	}
	
    // based on jena, but heavily modified. Jena allows e.g. % or _ as ns
    // delimiter. I only want #, : and /
    public static String getNs(String uri) {
        char ch;
        int lg = uri.length();
        if (lg == 0)
            return "";
        int i = lg - 1;
        for (; i >= 1; i--) {
            ch = uri.charAt(i);
            if (ch == '#' || ch == ':' || ch == '/')
                break;
        }

        int j = i + 1;

        if (j >= lg)
            return uri;

        return uri.substring(0, j);
    }

    /**
     * answer true iff this is not a legal NCName character, ie, is a possible
     * split-point start.
     */
    public static boolean notNameChar(char ch) {
        return !XMLChar.isNCName(ch);
    }

    private void getDataType() {
        if (stringRepresentation.contains("^^")) {
            // probably a datatype
            int closingQuote = stringRepresentation.lastIndexOf("\"");

            if (stringRepresentation.length() <= closingQuote + 2 || stringRepresentation.charAt(closingQuote + 1) != '^'
                    || stringRepresentation.charAt(closingQuote + 2) != '^' || stringRepresentation.charAt(closingQuote + 3) != '<') {
                // ah, no lang tag after all!! either nothing comes after the
                // quote, or something else than an '^^' follows
            } else {
                this.datatype = vault.store(stringRepresentation.substring(closingQuote + 4, stringRepresentation.length() - 1));
                this.dataTypeLength = stringRepresentation.length() - 1 - closingQuote - 4;
            }
        }
    }
	
	public boolean isDefinedClass() {
		return startsWithW3cUri() && (stringRepresentation.equals(RDF_CLASS) || stringRepresentation.equals(OWL_CLASS));
	}
	public boolean isDefinedProperty() {
		return startsWithW3cUri() && (stringRepresentation.equals(RDF_PROPERTY) || stringRepresentation.equals(OWL_DATATYPE_PROPERTY) || stringRepresentation.equals(OWL_OBJECT_PROPERTY));
	}

	private boolean startsWithW3cUri() {
		if (hasW3cUriPrefix == null) hasW3cUriPrefix = stringRepresentation.startsWith(W3C_URI_PREFIX);
		return hasW3cUriPrefix;
	}
	private void getLangTagInfo() {
		this.langTag = null;

		if (stringRepresentation.indexOf('@') >= 0) {
			//this is probably a literal

			int closingQuote = stringRepresentation.lastIndexOf("\"");
			if (stringRepresentation.length() == closingQuote + 1 || stringRepresentation.charAt(closingQuote+1) != '@') {
				//ah, no lang tag after all!! either nothing comes after the quote, or something else than an '@' follows
			} else {
			    this.langTag = vault.store(stringRepresentation.substring(closingQuote + 2, stringRepresentation.length()));
			    this.langTagLength = stringRepresentation.length() - 2 - closingQuote;
			}
		}
	}



	public String toString() {
		return
//			"ns: " + ns + "\n" +
			"datatype: " + datatype + "\n" +
			"lang tag: " + langTag + "\n" +
			"isLiteral: " + (isLiteral? "yes": "no") + "\n" +
			"isUri: " + (isUri? "yes": "no") + "\n" +
			"ignore: " + (ignoreIri? "yes": "no") + "\n";
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
	    Vault<String, PatriciaNode> vault = new PatriciaVault();
        NodeWrapper wrapper = new NodeWrapper(vault);
        wrapper.init("\"That Seventies Show\"^^<http://www.w3.org/2001/XMLSchema#string>", Position.OBJ);
        System.out.println(vault.redeem(wrapper.datatype));
        System.out.println(vault.redeem(wrapper.langTag));
        wrapper.init("\"That Seventies Show\"", Position.OBJ);
        System.out.println(vault.redeem(wrapper.datatype));
        System.out.println(vault.redeem(wrapper.langTag));
        wrapper.init("\"That Seventies Show\"@en", Position.OBJ);
        System.out.println(vault.redeem(wrapper.datatype));
        System.out.println(vault.redeem(wrapper.langTag));
        wrapper.init("\"That Seventies Show\"@en-be", Position.OBJ);
        System.out.println(vault.redeem(wrapper.datatype));
        System.out.println(vault.redeem(wrapper.langTag));
	    
	}
}

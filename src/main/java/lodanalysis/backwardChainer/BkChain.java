package lodanalysis.backwardChainer;

import java.lang.Exception;
import java.lang.StringBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.Scanner;

import lodanalysis.Entry;
import lodanalysis.RuneableClass;
import lodanalysis.backwardChainer.owl.Owl;
import lodanalysis.backwardChainer.rdf.Rdf;
import lodanalysis.backwardChainer.rdfs.Rdfs;
import data.Triple;

public class BkChain extends RuneableClass {

  public BkChain(Entry entry) {
		super(entry);
		System.out.println("test");
	}
private static long threshold     = 1000000;
  private static long tripleCounter = 0;
  private static Triple triple = null;
  private static Scanner input = null;

  private static Set<String> classSet    = new HashSet<String>();
  private static Set<String> propertySet = new HashSet<String>();
  private static Map<String, Set<String>> sameAsSubjectSet   = new HashMap<String, Set<String>>();
  private static Map<String, Set<String>> sameAsObjectSet   = new HashMap<String, Set<String>>();

  public static void main (String[] argv) {
    long tripleCounter = 0;
    String inputString = null;
    Triple inputTriple = null;
    String inputFile;

    if (argv.length < 1) {
      System.err.println ("give the input file!");
      System.exit(1);
    }

    inputFile = argv[0];

    try {
      BufferedReader br = new BufferedReader(new FileReader(inputFile));
      try {
        while ((inputString = br.readLine()) != null) {
          inputTriple = new Triple(inputString);
          filterTriple(inputTriple);
          updateTripleCounter();
        }
      } finally {
        br.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

//    printSets();
    checkoutSameAsChain(classSet);
    for (String type : classSet) {
      System.out.println (type);
    }
  }
  /* ----------------------------------- */
  private static void filterTriple (Triple triple) {
        filterClasses       (triple);
        filterProperties    (triple);
        filterSameAs        (triple);
        filterDomain        (triple);
        filterRange         (triple);
        filterSubClassOfAndEquClass       (triple);
        filterSubPropertyOfAndEquProperty (triple);
  }
  /* ----------------------------------- */
  private static void checkoutSameAsChain(Set<String> set) {

    List<String> temp = new ArrayList<String>();

    do {
      for (String c : set) {
        /* ----------------------------------- */
        if (sameAsSubjectSet.get(c) != null) {
          for (String s : sameAsSubjectSet.get(c)) {
            if (!set.contains(s)){
              temp.add(s);
            }
          }
        }
        /* ----------------------------------- */
        if (sameAsObjectSet.get(c) != null) {
          for (String s : sameAsObjectSet.get(c)) {
            if (!set.contains(s)){
              temp.add(s);
            }
          }
        }
      }
      /* ----------------------------------- */
      if (temp.size() > 0) {
        for (String s : temp) {
          set.add(s);
        }
        temp.clear();
      } else {
        break;
      }
    } while (true);
  }
  /* ----------------------------------- */
  private static void filterClasses (Triple triple) {
    if (triple.getObject().equals(Rdfs.CLASS)) {
          classSet.add(triple.getSubject());
    }
  }
  /* ----------------------------------- */
  private static void filterProperties (Triple triple) {
    if (triple.getObject().equals(Rdf.PROPERTY)) {
          propertySet.add(triple.getSubject());
    }
  }
  /* ----------------------------------- */
  private static void filterSameAs (Triple triple) {
    Set<String> objectSet = null;
    Set<String> subjectSet = null;

    if (triple.getPredicate().equals(Owl.SAMEAS)) {

      if (sameAsSubjectSet.get(triple.getSubject()) == null) {
        objectSet = new HashSet<String>();
        objectSet.add(triple.getObject());
        sameAsSubjectSet.put(triple.getSubject(), objectSet);
      } else {
        sameAsSubjectSet.get(triple.getSubject()).add(triple.getObject());
      }

      if (sameAsObjectSet.get(triple.getObject()) == null) {
        subjectSet = new HashSet<String>();
        subjectSet.add(triple.getSubject());
        sameAsObjectSet.put(triple.getObject(), subjectSet);
      } else {
        sameAsObjectSet.get(triple.getObject()).add(triple.getSubject());
      }
    }
  }
  /* ----------------------------------- */
  private static void filterSubClassOfAndEquClass (Triple triple) {
    if (triple.getPredicate().equals(Rdfs.SUBCLASSOF) ||
        triple.getPredicate().equals(Owl.EQUCLASS)) {
      classSet.add(triple.getSubject());
      classSet.add(triple.getObject());
    }
  }
  /* ----------------------------------- */
  private static void filterSubPropertyOfAndEquProperty (Triple triple) {
    if (triple.getPredicate().equals(Rdfs.SUBPROPERTYOF) ||
        triple.getPredicate().equals(Owl.EQUPROPERTY)) {
      propertySet.add(triple.getSubject());
      propertySet.add(triple.getObject());
    }
  }
  /* ----------------------------------- */
  private static void filterDomain (Triple triple) {
    if (triple.getPredicate().equals(Rdfs.DOMAIN)) {
      propertySet.add(triple.getSubject());
      classSet.add(triple.getObject());
    }
  }
  /* ----------------------------------- */
  private static void filterRange (Triple triple) {
    if (triple.getPredicate().equals(Rdfs.RANGE)) {
      propertySet.add(triple.getSubject());
      classSet.add(triple.getObject());
    }
  }
  /* ----------------------------------- */
  private static void filterDatatype (Triple triple) {
    if (triple.getPredicate().equals(Rdfs.DATATYPE)) {
      classSet.add(triple.getSubject());
    }
  }
  /* ----------------------------------- */
  private static void printSets() {
    for (Map.Entry<String, Set<String>> entry : sameAsSubjectSet.entrySet()) {
      System.out.println ("S key = " + entry.getKey());
      System.out.println ("S Value = " + entry.getValue());
    }
    for (Map.Entry<String, Set<String>> entry : sameAsObjectSet.entrySet()) {
      System.out.println ("O key = " + entry.getKey());
      System.out.println ("O Value = " + entry.getValue());
    }
  }
  /* ----------------------------------- */
  private static void updateTripleCounter() {
    if (tripleCounter % threshold == 0) {
      System.out.println (tripleCounter);
    }
    tripleCounter++;
  }
}

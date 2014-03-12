import java.lang.Exception;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Scanner;

import data.Triple;

public class BkChain extends Object {

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
    try {
      input = new Scanner (System.in);
      /* ----------------------------------- */
      while (input.hasNextLine()) {
          triple = new Triple(input.nextLine());

          filterTriple(triple);
          updateTripleCounter();
      }
      checkoutSameAsChain();
      //printSets();
      /* ----------------------------------- */
    } finally {
      if (input != null) {
        input.close();
      }
    }
  }
  /* ----------------------------------- */
  private static void filterTriple (Triple triple) {
        filterClasses    (triple);
        filterProperties (triple);
        filterSameAs     (triple);
        //filterDomain     (triple);
        //filterRange      (triple);
  }
  /* ----------------------------------- */
  private static void checkoutSameAsChain() {
    for (String c : classSet) {
      if (sameAsSubjectSet.get(c) != null) {
        for (String s : sameAsSubjectSet.get(c)) {
          System.out.println (s);
        }
      } else if (sameAsObjectSet.get(c) != null) {
        for (String s : sameAsObjectSet.get(c)) {
          System.out.println (s);
        }
      }
    }
  }
  /* ----------------------------------- */
  private static void filterClasses (Triple triple) {
    if (triple.getObject().matches(".*#Class>")) {
          classSet.add(triple.getSubject());
    }
  }
  /* ----------------------------------- */
  private static void filterProperties (Triple triple) {
    if (triple.getObject().matches(".*#Property>")) {
          propertySet.add(triple.getSubject());
    }
  }
  /* ----------------------------------- */
  private static void filterSameAs (Triple triple) {
    Set<String> objectSet = null;
    Set<String> subjectSet = null;

    if (triple.getPredicate().matches(".*owl#sameAs>")) {

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
  private static void printSets() {
    for (Map.Entry<String, Set<String>> entry : sameAsSubjectSet.entrySet()) {
      System.out.println ("key = " + entry.getKey());
      System.out.println ("Value = " + entry.getValue());
    }
  }
  /* ----------------------------------- */
  private static void updateTripleCounter() {
    if (tripleCounter % threshold == 0) {
      System.out.println (tripleCounter + "M");
    }
    tripleCounter++;
  }
}

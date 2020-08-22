package com.rpeck.coda;

import java.io.*;
import java.util.*;

public class FooCorpKVStore {

  /**
   * A stack of value HashMaps, one per transactional valuesContext.
   */
  private List<HashMap<String, String>> valueStack;

  /**
   * A stack of pending delete HashMaps, one per transactional valuesContext.
   */
  private List<HashMap<String, Boolean>> pendingDeleteStack;

  public FooCorpKVStore() {
    clear();
    push();
  }

  private HashMap<String, String> valuesContext() {
    return valueStack.get(valueStack.size() - 1);
  }

  private HashMap<String, Boolean> deletesContext() {
    return pendingDeleteStack.get(pendingDeleteStack.size() - 1);
  }

  private boolean inATransaction() {
    return (valueStack.size() > 1);
  }

  private boolean inANestedTransaction() {
    return (valueStack.size() > 2);
  }

  private void clear() {
    valueStack = new ArrayList<>();
    pendingDeleteStack = new ArrayList<>();
  }

  private void pop() {
    valueStack.remove(valueStack.size() - 1);
    pendingDeleteStack.remove(valueStack.size() - 1);
  }

  private void push() {
    valueStack.add(new HashMap<String, String>());
    pendingDeleteStack.add(new HashMap<String, Boolean>());
  }


  public String set(String key, String value) {
    HashMap<String, String> context = valuesContext();
    String oldVal = context.get(key);
    deletesContext().remove(key);  // correctly handle DELETE followed by SET
    context.put(key, value);
    return oldVal;
  }

  public String get(String key) {
    // TODO: optimize reads so they are O(1) rather than O(contextDepth)
    for (int index = valueStack.size() - 1; index >= 0; index--) {
      //check for a pending DELETE
      HashMap<String, Boolean> aDeletes = pendingDeleteStack.get(index);
      if (aDeletes.containsKey(key)) {
        return null;
      }

      // check for a value
      HashMap<String , String> aContext = valueStack.get(index);
      if (aContext.containsKey(key)) {
        return aContext.get(key);
      }
    }
    System.err.println(key + " not set");
    return null;
  }

  public void delete(String key) {
    if (inATransaction()) {
      deletesContext().put(key, true);
    } else {
      valuesContext().remove(key);
    }
  }

  // TODO!
  public int count(String value) {
    // don't double-count A set to 1 in multiple frames!
    return 0;
  }

  public void begin() {
    push();
  }

  public void commit() {
    if (! inATransaction()) {
      System.out.println("NO TRANSACTION");
      return;
    }

    HashMap<String, String> priorContext = valueStack.get(valueStack.size() - 2);
    priorContext.putAll(valuesContext());

    // handle deletes
    HashMap<String, Boolean> priorDeletes = pendingDeleteStack.get(valueStack.size() - 2);
    for (String removedKey : deletesContext().keySet()) {
      // If we're in a nested frame we add to the prior frame's pending deletes,
      // else we delete "for real".
      if (inANestedTransaction()) {
        priorDeletes.put(removedKey, true);
      } else {
        priorContext.remove(removedKey);
      }
    }
    pop();
  }

  public void rollback() {
    if (! inATransaction()) {
      System.out.println("NO TRANSACTION");
      return;
    }

    pop();
  }

  public void repl(InputStream rawIn, PrintStream out) {
    clear();
    push();

    BufferedReader br = new BufferedReader(new InputStreamReader(rawIn));

    try {
      while (true) {
        // TODO: handle EOF!
        String inputLine = br.readLine();
        // out.println(inputLine);
        StringTokenizer st = new StringTokenizer(inputLine);

      }
    } catch (IOException e) {
        System.err.println(e);
        return;
      }
  }

  private void expect(String a, String b) throws RuntimeException {

    if (a == null && b == null) return;
    if (a == null) throw new RuntimeException(a + " != " + b);
    if (! a.equals(b)) throw new RuntimeException(a + " != " + b);
    return;
  }

  /**
   *
   * Destructive test of the KV store.
   */
  public boolean test() {
    clear();
    push();
    set("A", "2");
    expect("2", get("A"));
    set("A", "3");
    expect("3", get("A"));

    clear();
    push();
    set("A", "2");
    expect("2", get("A"));
    delete("A");
    delete("B");
    expect(null, get("A"));

    clear();
    push();
    set("A", "2");
    expect("2", get("A"));
    expect(null, get("B"));

    // TODO: count() test

    clear();
    push();
    commit();  // NO TRANSACTION
    begin();
    set("A", "1");
    expect("1", get("A"));
    commit();
    expect("1", get("A"));

    clear();
    push();
    rollback();  // NO TRANSACTION
    begin();
    set("A", "1");
    expect("1", get("A"));
    rollback();
    expect(null, get("A")); // A not set



    System.out.println("Test succeeded.");
    clear();
    return true;
  }

  public static void main(String[] args) {
	  System.out.println("Yo!");

	  FooCorpKVStore kvStore = new FooCorpKVStore();
	  if (! kvStore.test()) {
	    System.err.println("KVStore failed the test; exiting.");
	    System.exit(-1);
    }

    kvStore.repl(System.in, System.out);
  }
}

package com.maxmind.db;

import java.net.InetAddress;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Callbacks for the low-allocation database query interface.
 * This lets you build a specification of which object paths you want callback for.
 */
public class Callbacks {

    public static interface Callback<X> {}

    @FunctionalInterface
    public static interface TextNode<X> extends Callback<X> {
	public abstract void setValue(X state, CharSequence value);

	// Utility function:
	public static void assignToStringBuilder(StringBuilder sb, CharSequence value) {
	    sb.setLength(0);
	    sb.append(value);
	}
    }

    @FunctionalInterface
    public static interface DoubleNode<X> extends Callback<X> {
	public abstract void setValue(X state, double value);
    }

    @FunctionalInterface
    /** Like BiConsumer&lt;X,Integer&gt; but avoiding boxing. */
    public static interface ArrayBeginCallback<X> {
	public abstract void accept(X state, int size);
    }
    @FunctionalInterface
    public static interface ArrayElementCallback<X> {
	public abstract Callback<X> callbackForPosition(X state, int index, int size);
    }

    public abstract static class ArrayNode<X> implements Callback<X> {
	public ArrayNode() {}

	public void arrayBegin(X state, int size) {}
	public abstract Callback<X> callbackForElement(X state, int i, int size);
	public void arrayEnd(X state) {}
    }

    public static class ObjectNode<X> implements Callback<X> {
	private final Map<CharSequence, Callback<X>> fieldsOfInterest;

	public ObjectNode(Map<String, Callback<X>> fieldsOfInterest) {
	    Map<CharSequence, Callback<X>> mapToUse = new TreeMap<>(CHARSEQ_COMPARATOR);
	    mapToUse.putAll(fieldsOfInterest);
	    this.fieldsOfInterest = mapToUse;
	}

	public Callback<X> callbackForField(CharSequence field) {
	    return fieldsOfInterest.get(field);
	}

	public void objectBegin(X state) {}
	public void objectEnd(X state) {}
    }

    public static class RecordCallback<X> extends ObjectNode<X> {
	public RecordCallback(Map<String, Callback<X>> fieldsOfInterest) {
	    super(fieldsOfInterest);
	}

	public void network(X state, byte[] ipAddress, int prefixLength) {}
    }


    private static class CharSequenceComparator implements Comparator<CharSequence> {
	public int compare(CharSequence a, CharSequence b) {
	    int lenA = a.length(), lenB = b.length();
	    int minLen = Math.min(lenA, lenB);

	    int i = 0;
	    while (i < minLen) {
		char cA = a.charAt(i);
		char cB = b.charAt(i);
		if (cA != cB) {
		    return cA<cB ? -1 : 1;
		}
		i++;
	    }

	    if (lenA != lenB) {
		return lenA < lenB ? -1 : 1;
	    }
	    return 0;
	}
    }

    private static final Comparator<CharSequence> CHARSEQ_COMPARATOR = new CharSequenceComparator();

    public static class ObjectCallbackBuilder<X> {
	private final Map<String, Callback<X>> here = new HashMap<>();
	private final Map<String, ObjectCallbackBuilder<X>> deeper = new HashMap<>();
	protected Consumer<X> onBegin = null, onEnd = null;

	public ObjectCallbackBuilder() {}

	public void text(String key, TextNode<X> callback) {
	    if (deeper.containsKey(key)) throw new IllegalStateException("An inner object is already registered here: '"+key+"'");
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");
	    here.put(key, callback);
	}

	public void number(String key, DoubleNode<X> callback) {
	    if (deeper.containsKey(key)) throw new IllegalStateException("An inner object is already registered here: '"+key+"'");
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");
	    here.put(key, callback);
	}

	public void onBegin(Consumer<X> callback) {
	    if (onBegin != null) throw new IllegalStateException("An onBegin callback is already registered on this object.");
	    onBegin = callback;
	}

	public void onEnd(Consumer<X> callback) {
	    if (onEnd != null) throw new IllegalStateException("An onEnd callback is already registered on this object.");
	    onEnd = callback;
	}

	private ObjectCallbackBuilder<X> getOrCreateSubObject(String key) {
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");
	    ObjectCallbackBuilder<X> r = deeper.get(key);
	    if (r == null) {
		deeper.put(key, r = new ObjectCallbackBuilder<X>());
	    }
	    return r;
	}

	public void array(String key,
			  final ArrayBeginCallback<X> onBegin,
			  final ArrayElementCallback<X> onElementCallback,
			  final Consumer<X> onEnd)
	{
	    if (deeper.containsKey(key)) throw new IllegalStateException("An inner object is already registered here: '"+key+"'");
	    if (here.containsKey(key)) throw new IllegalStateException("Another callback is already registered here: '"+key+"'");


	    // TODO Perhaps introduce an ArrayCallbackBuilder if the flexibility is desired.
	    ArrayNode<X> callback = new ArrayNode<X>() {
	        @Override public void arrayBegin(X state, int size) {
		    if (onBegin != null) onBegin.accept(state, size);
		}
		@Override public void arrayEnd(X state) {
		    if (onEnd != null) onEnd.accept(state);
		}
		@Override public Callback<X> callbackForElement(X state, int i, int size) {
		    return onElementCallback.callbackForPosition(state, i, size);
		}
	    };

	    here.put(key, callback);
	}

	public ObjectCallbackBuilder<X> obj(String key) {
	    ObjectCallbackBuilder<X> subObject = getOrCreateSubObject(key);
	    return subObject;
	}

	public ObjectNode<X> build() {
	    final Consumer<X> onBegin = this.onBegin;
	    final Consumer<X> onEnd = this.onEnd;
	    return new ObjectNode<X>(buildMap()) {
		@Override public void objectBegin(X state) {
		    if (onBegin != null) onBegin.accept(state);
		}
		@Override public void objectEnd(X state) {
		    if (onEnd != null) onEnd.accept(state);
		}
	    };
	}

	protected Map<String, Callback<X>> buildMap() {
	    Map<String, Callback<X>> fieldsOfInterest = new HashMap<>(here);
	    for (Map.Entry<String, ObjectCallbackBuilder<X>> e : deeper.entrySet()) {
		fieldsOfInterest.put(e.getKey(), e.getValue().build());
	    }
	    return fieldsOfInterest;
	}
    }

    public static class RecordCallbackBuilder<X> extends ObjectCallbackBuilder<X> {
	private RecordNetworkCallback<X> onNetwork = null;

	public RecordCallbackBuilder() {}

	public void onNetwork(RecordNetworkCallback<X> callback) {
	    if (onNetwork != null) throw new IllegalStateException("An onNetwork callback is already registered on this object.");
	    onNetwork = callback;
	}

	@Override
    	public RecordCallback<X> build() {
	    final RecordNetworkCallback<X> onNetwork = this.onNetwork;
	    final Consumer<X> onBegin = this.onBegin;
	    final Consumer<X> onEnd = this.onEnd;
	    return new RecordCallback<X>(buildMap()) {
		@Override public void network(X state, byte[] ipAddress, int prefixLength) {
		    if (onNetwork != null) onNetwork.network(state, ipAddress, prefixLength);
		}
		@Override public void objectBegin(X state) {
		    if (onBegin != null) onBegin.accept(state);
		}
		@Override public void objectEnd(X state) {
		    if (onEnd != null) onEnd.accept(state);
		}
	    };
	}
    }

    @FunctionalInterface
    public interface RecordNetworkCallback<X> {
 	public void network(X state, byte[] ipAddress, int prefixLength);
    }


}

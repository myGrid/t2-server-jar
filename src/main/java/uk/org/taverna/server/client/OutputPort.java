package uk.org.taverna.server.client;

import java.net.URI;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.runtime.builtin.IRubyObject;

import uk.org.taverna.server.client.util.TreeList;

public final class OutputPort extends Port {

	private static final long serialVersionUID = 1L;

	private TreeList<PortValue> structure = null;

	private OutputPort(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new OutputPort(runtime, metaclass);
	}

	public boolean isError() {
		return getValue().isError();
	}

	public String getError() {
		return getValue().getError();
	}

	public String getError(int... coords) throws IndexOutOfBoundsException {
		return getValue(coords).getError();
	}

	public String getDataType() {
		return getValue().getDataType();
	}

	public String getDataType(int... coords) throws IndexOutOfBoundsException {
		return getValue(coords).getDataType();
	}

	public int getDataSize() {
		return getValue().getDataSize();
	}

	public int getDataSize(int... coords) throws IndexOutOfBoundsException {
		return getValue(coords).getDataSize();
	}

	public byte[] getData() {
		return getValue().getData();
	}

	public byte[] getData(int from, int to, boolean isExclusive) {
		return getValue().getData(from, to, isExclusive);
	}

	public byte[] getData(int... coords) throws IndexOutOfBoundsException {
		return getValue(coords).getData();
	}

	public PortValue getValue() {
		if (structure == null) {
			parsePortStructure();
		}

		return structure.getValue();
	}

	public PortValue getValue(int... coords) throws IndexOutOfBoundsException {
		int depth = getDepth();
		if (coords.length != depth) {
			throw new IndexOutOfBoundsException("OutputPort of depth " + depth
					+ " accessed as depth " + coords.length);
		}

		if (structure == null) {
			parsePortStructure();
		}

		return structure.getValue(coords);
	}

	public URI getReference() {
		return getValue().getReference();
	}

	public URI getReference(int coords) throws IndexOutOfBoundsException {
		return getValue(coords).getReference();
	}

	private void parsePortStructure() {
		if (getDepth() == 0) {
			PortValue value = (PortValue) callRubyMethod("get_structure",
					PortValue.class);
			structure = new TreeList<PortValue>(value);
		} else {
			RubyArray values = (RubyArray) callRubyMethod("get_structure",
					RubyArray.class);
			structure = parsePortStructure(values, new TreeList<PortValue>());
		}
	}

	private TreeList<PortValue> parsePortStructure(RubyArray in,
			TreeList<PortValue> struct) {
		for (Object o : in) {
			if (o.getClass() == RubyArray.class) {
				struct.addNode(parsePortStructure((RubyArray) o,
						new TreeList<PortValue>()));
			} else {
				PortValue value = (PortValue) ((IRubyObject) o)
						.toJava(PortValue.class);
				struct.addNode(new TreeList<PortValue>(value));
			}
		}

		return struct;
	}

	@Override
	public String toString() {
		if (structure == null) {
			parsePortStructure();
		}

		return structure.toString();
	}
}

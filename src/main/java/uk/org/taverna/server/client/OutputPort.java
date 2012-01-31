package uk.org.taverna.server.client;

import java.net.URI;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyRange;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.builtin.IRubyObject;

public final class OutputPort extends Port {

	private static final long serialVersionUID = 1L;

	private OutputPort(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new OutputPort(runtime, metaclass);
	}

	public boolean isError() {
		return (Boolean) callRubyMethod("error?", boolean.class);
	}

	public byte[] getValue() {
		String value = (String) callRubyMethod("value", String.class);

		return value.getBytes();
	}

	public byte[] getValue(int from, int to, boolean isExclusive) {
		IRubyObject begin = JavaUtil.convertJavaToRuby(runtime, from);
		IRubyObject end = JavaUtil.convertJavaToRuby(runtime, to);
		RubyRange range = RubyRange.newRange(runtime,
				runtime.getCurrentContext(), begin, end, isExclusive);

		String value = (String) callRubyMethod("value", String.class, range);

		return value.getBytes();
	}

	public void getValue(int... coords) {
		// String method = "[";
		// int dims = coords.length;
		// for (int c : coords) {
		// method += c + "]";
		// if (dims > 1) {
		// method += "[";
		// dims--;
		// }
		// }

		// Maybe should grab and parse out from @structure into
		// TreeList<PortValue> ?

		RubyArray ra = (RubyArray) callRubyMethod("get_structure",
				RubyArray.class);

		System.out.println(ra.toString());

		// PortValue value = (PortValue) callRubyMethod(method,
		// PortValue.class);
		//
		// return value.getValue();
	}

	public URI getReference() {
		String ref = (String) callRubyMethod("ref", String.class);

		return URI.create(ref);
	}
}

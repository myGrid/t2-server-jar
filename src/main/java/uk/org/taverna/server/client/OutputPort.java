package uk.org.taverna.server.client;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyRange;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.builtin.IRubyObject;

public final class OutputPort extends Port {

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

	public PortValue getValue(int... coords) {
		String method = "[";
		int dims = coords.length;
		for (int c : coords) {
			method += c + "]";
			if (dims > 1) {
				method += "[";
				dims--;
			}
		}

		return (PortValue) callRubyMethod(method, PortValue.class);
	}
}

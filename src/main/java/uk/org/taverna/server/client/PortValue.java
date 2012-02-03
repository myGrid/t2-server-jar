package uk.org.taverna.server.client;

import java.net.URI;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyRange;
import org.jruby.RubyString;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.builtin.IRubyObject;

public class PortValue extends JRubyBase {

	private static final long serialVersionUID = 1L;

	private PortValue(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new PortValue(runtime, metaclass);
	}

	public int getDataSize() {
		return (Integer) callRubyMethod("size", int.class);
	}

	public String getDataType() {
		return (String) callRubyMethod("type", String.class);
	}

	public byte[] getData() {
		RubyString value = (RubyString) callRubyMethod("value",
				RubyString.class);

		return value.getBytes();
	}

	public byte[] getData(int from, int to, boolean isExclusive) {
		IRubyObject begin = JavaUtil.convertJavaToRuby(runtime, from);
		IRubyObject end = JavaUtil.convertJavaToRuby(runtime, to);
		RubyRange range = RubyRange.newRange(runtime,
				runtime.getCurrentContext(), begin, end, isExclusive);

		RubyString value = (RubyString) callRubyMethod("value",
				RubyString.class, range);

		return value.getBytes();
	}

	public byte[] getData(int start, int length) {
		return getData(start, (start + length), true);
	}

	public URI getReference() {
		String ref = (String) callRubyMethod("ref", String.class);

		return URI.create(ref);
	}

	public boolean isError() {
		return (Boolean) callRubyMethod("error?", boolean.class);
	}

	public String getError() {
		return (String) callRubyMethod("error", String.class);
	}

	@Override
	public String toString() {
		return toString(false);
	}

	public String toString(boolean reference) {
		if (reference) {
			return getReference().toString();
		} else if (isError()) {
			return "Error: " + getError();
		} else {
			return new String(getData());
		}
	}
}

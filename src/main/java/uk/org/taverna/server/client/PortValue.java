package uk.org.taverna.server.client;

import org.jruby.Ruby;
import org.jruby.RubyClass;
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

	public byte[] getValue() {
		String value = (String) callRubyMethod("value", String.class);

		return value.getBytes();
	}
}

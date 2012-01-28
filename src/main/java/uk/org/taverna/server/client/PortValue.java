package uk.org.taverna.server.client;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.runtime.builtin.IRubyObject;

public class PortValue extends JRubyBase {

	private PortValue(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new PortValue(runtime, metaclass);
	}

	public int getDataSize() {
		return (Integer) callRubyMethod("size", int.class);
	}
}

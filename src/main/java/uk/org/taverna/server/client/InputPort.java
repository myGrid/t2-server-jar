package uk.org.taverna.server.client;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.runtime.builtin.IRubyObject;

public final class InputPort extends Port {

	private InputPort(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new InputPort(runtime, metaclass);
	}

	public boolean isSet() {
		return (Boolean) callRubyMethod("set?", boolean.class);
	}

	public String getValue() {
		return (String) callRubyMethod("value", String.class);
	}

	public void setValue(Object value) {
		callRubyMethod("value=", value.toString());
	}
}

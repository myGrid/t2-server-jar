/*
 * Copyright (c) 2010-2012 The University of Manchester, UK.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * * Neither the names of The University of Manchester nor the names of its
 *   contributors may be used to endorse or promote products derived from this
 *   software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package uk.org.taverna.server.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.javasupport.JavaUtil;
import org.jruby.javasupport.util.RuntimeHelpers;
import org.jruby.runtime.builtin.IRubyObject;

class JRubyBase extends RubyObject implements Constants {

	protected static final Ruby runtime = Ruby.getGlobalRuntime();
	private static final Map<String, RubyClass> metaclasses = new HashMap<String, RubyClass>();

	static {
		runtime.getLoadService().lockAndRequire(Constants.RUBY_CODE);

		// pre-register all the classes we will be using
		for (Class<?> c : Constants.CLASSES) {
			String name = c.getSimpleName();
			RubyClass metaclass = runtime.getModule(Constants.MODULE).getClass(
					name);
			metaclass.setRubyStaticAllocator(c);
			metaclasses.put(name, metaclass);
		}
	}

	protected JRubyBase(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	protected static RubyClass getMetaClass(String name) {
		return metaclasses.get(name);
	}

	protected void callRubyMethod(IRubyObject self, String method) {
		RuntimeHelpers.invoke(runtime.getCurrentContext(), self, method);
	}

	protected void callRubyMethod(String method) {
		callRubyMethod(this, method);
	}

	protected Object callRubyMethod(IRubyObject self, String method,
			Class<?> returnType) {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, method);
		return result.toJava(returnType);
	}

	protected Object callRubyMethod(String method, Class<?> returnType) {
		return callRubyMethod(this, method, returnType);
	}

	protected void callRubyMethod(IRubyObject self, String method,
			Object... args) {

		List<IRubyObject> rArgs = new ArrayList<IRubyObject>();
		for(Object o : args) {
			IRubyObject arg = JavaUtil.convertJavaToRuby(runtime, o);
			rArgs.add(arg);
		}

		RuntimeHelpers.invoke(runtime.getCurrentContext(), this, method,
				rArgs.toArray(new IRubyObject[0]));
	}

	protected void callRubyMethod(String method, Object... args) {
		callRubyMethod(this, method, args);
	}

	protected Object callRubyMethod(IRubyObject self, String method,
			Class<?> returnType, Object... args) {

		List<IRubyObject> rArgs = new ArrayList<IRubyObject>();
		for (Object o : args) {
			IRubyObject arg = JavaUtil.convertJavaToRuby(runtime, o);
			rArgs.add(arg);
		}

		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, method, rArgs.toArray(new IRubyObject[0]));

		return result.toJava(returnType);
	}

	protected void callRubyMethod(String method, Class<?> returnType,
			Object... args) {
		callRubyMethod(this, method, returnType, args);
	}
}

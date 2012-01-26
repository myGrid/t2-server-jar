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

import java.util.Date;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubySymbol;
import org.jruby.javasupport.JavaUtil;
import org.jruby.javasupport.util.RuntimeHelpers;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * The Run class represents a workflow run on a Taverna Server instance. It is
 * created by supplying a Server instance on which to create it and a workflow
 * to be run.
 * 
 * @author Robert Haines
 */
public final class Run extends JRubyBase {

	private Run(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new Run(runtime, metaclass);
	}

	/**
	 * Create a new Run instance on the specified server with the supplied
	 * workflow.
	 * 
	 * @param server
	 *            The server to create the Run on.
	 * @param workflow
	 *            The workflow associated with the Run.
	 */
	public static Run create(Server server, String workflow,
			Credentials credentials) {
		IRubyObject rServer = JavaUtil.convertJavaToRuby(runtime, server);
		IRubyObject rWorkflow = JavaUtil.convertJavaToRuby(runtime, workflow);
		IRubyObject rCreds = JavaUtil.convertJavaToRuby(runtime, credentials);

		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				getMetaClass("Run"),
				"create", rServer, rWorkflow, rCreds);

		return (Run) result.toJava(Run.class);
	}

	/**
	 * Create a new Run instance on the specified server with the supplied
	 * workflow file.
	 * 
	 * @param server
	 *            The server to create the Run on.
	 * @param workflow
	 *            The file containing the workflow to be associated with the
	 *            Run.
	 */
	// public Run(Server server, File workflow, HttpBasic credentials)
	// throws IOException {
	// this(server, FileUtils.readFileToString(workflow), credentials);
	// }

	public static Run create(String server, String workflow,
			Credentials credentials) {
		return Run.create(new Server(server), workflow, credentials);
	}

	// static Run create(Server server, Credentials credentials, String id) {
	// return Run.create(server, "", id, credentials);
	// }

	public void delete() {
		RuntimeHelpers.invoke(runtime.getCurrentContext(), this, "delete");
	}

	public String getIdentifier() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "uuid");
		return (String) result.toJava(String.class);
	}

	public Date getCreateTime() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "create_time");
		return (Date) result.toJava(Date.class);
	}

	public Date getStartTime() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "start_time");
		return (Date) result.toJava(Date.class);
	}

	public Date getFinishTime() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "finish_time");
		return (Date) result.toJava(Date.class);
	}

	public void start() {
		RuntimeHelpers.invoke(runtime.getCurrentContext(), this, "start");
	}

	public void wait(boolean progress) {
		IRubyObject rProgress = JavaUtil.convertJavaToRuby(runtime, progress);
		RubySymbol progLabel = RubySymbol.newSymbol(runtime, "progress");
		RubyHash args = RuntimeHelpers.constructHash(runtime, progLabel,
				rProgress);

		RuntimeHelpers.invoke(runtime.getCurrentContext(), this, "wait", args);
	}

	public String getConsoleOutput() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "stdout");
		return (String) result.toJava(String.class);
	}

	public String getConsoleError() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "stderr");
		return (String) result.toJava(String.class);
	}

	public int getExitCode() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "exitcode");
		return (Integer) result.toJava(int.class);
	}

	public String getWorkflow() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "workflow");
		return (String) result.toJava(String.class);
	}

	public Date getExpiryTime() {
		IRubyObject result = RuntimeHelpers.invoke(runtime.getCurrentContext(),
				this, "expiry");
		return (Date) result.toJava(Date.class);
	}
}

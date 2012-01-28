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

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
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

	public void delete() {
		callRubyMethod("delete");
	}

	public String getIdentifier() {
		return (String) callRubyMethod(this, "uuid", String.class);
	}

	public Date getCreateTime() {
		return (Date) callRubyMethod("create_time", Date.class);
	}

	public Date getStartTime() {
		return (Date) callRubyMethod("start_time", Date.class);

	}

	public Date getFinishTime() {
		return (Date) callRubyMethod("finish_time", Date.class);

	}

	public void start() {
		callMethod("start");
	}

	public String getConsoleOutput() {
		return (String) callRubyMethod("stdout", String.class);
	}

	public String getConsoleError() {
		return (String) callRubyMethod("stderr", String.class);

	}

	public int getExitCode() {
		return (Integer) callRubyMethod("exitcode", int.class);
	}

	public String getWorkflow() {
		return (String) callRubyMethod("workflow", String.class);
	}

	public Date getExpiryTime() {
		return (Date) callRubyMethod("expiry", Date.class);
	}

	public void setExpiryTime(Date expiry) {
		callRubyMethod("expiry=", expiry.toString());
	}

	public RunStatus getStatus() {
		String status = (String) callRubyMethod("status", String.class);

		return RunStatus.state(status);
	}

	public boolean isInitialized() {
		return getStatus() == RunStatus.INITIALIZED;
	}

	public boolean isRunning() {
		return getStatus() == RunStatus.RUNNING;
	}

	public boolean isFinished() {
		return getStatus() == RunStatus.FINISHED;
	}

	public Map<String, InputPort> getInputPorts() {
		RubyHash rh = (RubyHash) callRubyMethod("input_ports", RubyHash.class);
		if (rh.isNil() || rh.isEmpty()) {
			return null;
		} else {
			Map<String, InputPort> ports = new HashMap<String, InputPort>();
			String[] keys = (String[]) rh.keys().toArray(new String[0]);
			for (String key : keys) {
				IRubyObject p = (IRubyObject) rh.get(key);
				InputPort port = (InputPort) p.toJava(InputPort.class);
				ports.put(key, port);
			}

			return ports;
		}
	}

	public void setInput(String input, Object value) {
		callRubyMethod("set_input", input, value.toString());
	}

	public void setBaclavaInput(File baclavaFile) {
		String filename = baclavaFile.getPath();
		callRubyMethod("upload_baclava_input", filename);
	}

	public Map<String, OutputPort> getOutputPorts() {
		RubyHash rh = (RubyHash) callRubyMethod("output_ports", RubyHash.class);
		if (rh.isNil() || rh.isEmpty()) {
			return null;
		} else {
			Map<String, OutputPort> ports = new HashMap<String, OutputPort>();
			String[] keys = (String[]) rh.keys().toArray(new String[0]);
			for (String key : keys) {
				IRubyObject p = (IRubyObject) rh.get(key);
				OutputPort port = (OutputPort) p.toJava(OutputPort.class);
				ports.put(key, port);
			}

			return ports;
		}
	}
}

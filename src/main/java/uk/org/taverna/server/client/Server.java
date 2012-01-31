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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.Block;
import org.jruby.runtime.builtin.IRubyObject;

/**
 * The Server class represents a connection to a Taverna Server instance
 * somewhere on the Internet. Only one instance of this class is created for
 * each Taverna Server instance.
 * 
 * To make a connection to a server call {@link Server#connect(URI)} with it
 * full URL as the parameter. If there already exists a Server instance that is
 * connected to this Taverna Server then it will be returned, otherwise a new
 * Server instance is created and returned.
 * 
 * @author Robert Haines
 */
public final class Server extends JRubyBase {

	private static final long serialVersionUID = 1L;

	public Server(String uri) {
		super(runtime, getMetaClass("Server"));
		IRubyObject rUri = JavaUtil.convertJavaToRuby(runtime, uri);

		callInit(rUri, Block.NULL_BLOCK);
	}

	public Server(URI uri) {
		this(uri.toASCIIString());
	}

	private Server(Ruby runtime, RubyClass metaclass) {
		super(runtime, metaclass);
	}

	public static IRubyObject __allocate__(Ruby runtime, RubyClass metaclass) {
		return new Server(runtime, metaclass);
	}

	public URI getUri() {
		return URI.create(getStringUri());
	}

	public String getStringUri() {
		Object o = callRubyMethod("uri", Object.class);

		return o.toString();
	}

	public float getVersion() {
		return (Float) callRubyMethod("version", float.class);
	}

	public int getRunLimit(Credentials credentials) {
		return (Integer) callRubyMethod("run_limit", int.class,
				credentials);
	}

	public Collection<Run> getRuns(Credentials credentials) {
		Run[] runs = (Run[]) callRubyMethod("runs", Run[].class,
				credentials);

		return Arrays.asList(runs);
	}

	public Run getRun(String identifier, Credentials credentials) {
		Run run = (Run) callRubyMethod("run", Run.class, identifier,
				credentials);

		if (run == null) {
			throw new RunNotFoundException(identifier);
		}

		return run;
	}

	public Run createRun(String workflow, Credentials credentials) {
		return Run.create(this, workflow, credentials);
	}

	public void deleteRun(String identifier, Credentials credentials) {
		callRubyMethod("delete_run", identifier, credentials);
	}

	public void deleteRun(Run run, Credentials credentials) {
		deleteRun(run.getIdentifier(), credentials);
	}

	public void deleteAllRuns(Credentials credentials) {
		callRubyMethod("delete_all_runs", credentials);
	}
}

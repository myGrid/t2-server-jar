/*
 * Copyright (c) 2012 The University of Manchester, UK.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import org.junit.Test;

import uk.org.taverna.server.client.util.URIUtils;

public class TestSecureWorkflows extends TestRunsBase {

	// Secure workflows
	private final static String WKF_BASIC_HTTP = "/workflows/secure/basic-http.t2flow";
	private final static String WKF_DIGEST_HTTP = "/workflows/secure/digest-http.t2flow";
	private final static String WKF_WS_HTTP = "/workflows/secure/ws-http.t2flow";

	// Service endpoints
	private final static URI HEATER_HTTP = URI
			.create("http://heater.cs.man.ac.uk:7070/");

	// SOAP methods
	private final static String WS1 = "axis/services/HelloService-PlaintextPassword";
	private final static String WS2 = "axis/services/HelloService-DigestPassword";
	private final static String WS3 = "axis/services/HelloService-PlaintextPassword-Timestamp";
	private final static String WS4 = "axis/services/HelloService-DigestPassword-Timestamp";
	private final static String WS[] = { WS1, WS2, WS3, WS4 };

	// Credentials
	private final static String USERNAME = "testuser";
	private final static String PASSWORD = "testpasswd";

	@Test
	public void testNoCreds() {
		byte[] workflow = loadResource(WKF_BASIC_HTTP);
		Run run = server.createRun(workflow, user1);
		try {
			run.start();
		} catch (Exception e) {
			fail("Failed to start run.");
		}

		assertTrue("Run has finished", run.isRunning());
		wait(run);
		assertTrue("Run has finished", run.isFinished());

		assertEquals("Run has no output", 0, run.getOutputPort("out")
				.getDataSize());
	}

	@Test
	public void testHTTPBasicAuth() {
		testRestHTTP(WKF_BASIC_HTTP);
	}

	@Test
	public void testHTTPDigestAuth() {
		testRestHTTP(WKF_DIGEST_HTTP);
	}

	@Test
	public void testWSCredsHTTP() {
		byte[] workflow = loadResource(WKF_WS_HTTP);
		Run run = server.createRun(workflow, user1);
		URI uri;

		for (String ws : WS) {
			uri = buildWSURI(HEATER_HTTP, ws);
			run.setServiceCredential(uri, USERNAME, PASSWORD);
		}

		try {
			run.start();
		} catch (Exception e) {
			fail("Failed to start run.");
		}

		assertTrue("Run has finished", run.isRunning());
		wait(run);
		assertTrue("Run has finished", run.isFinished());

		assertEquals("Plaintext output", "Hello Alan!",
				run.getOutputPort("out_plaintext").getDataAsString());
		assertEquals("Digest output", "Hello Stian!",
				run.getOutputPort("out_digest").getDataAsString());
		assertEquals("Timestamped plaintext output", "Hello Alex!", run
				.getOutputPort("out_plaintext_timestamp").getDataAsString());
		assertEquals("Timestamped digest output", "Hello David!", run
				.getOutputPort("out_digest_timestamp").getDataAsString());
	}

	private void testRestHTTP(String filename) {
		byte[] workflow = loadResource(filename);
		Run run = server.createRun(workflow, user1);

		run.setServiceCredential(HEATER_HTTP, USERNAME, PASSWORD);

		try {
			run.start();
		} catch (Exception e) {
			fail("Failed to start run.");
		}

		assertTrue("Run has finished", run.isRunning());
		wait(run);
		assertTrue("Run has finished", run.isFinished());

		assertTrue("Run has an output",
				run.getOutputPort("out").getDataSize() > 0);
	}

	private URI buildWSURI(URI uri, String path) {
		return URIUtils.setQuery(URIUtils.appendToPath(uri, path), "wsdl");
	}
}

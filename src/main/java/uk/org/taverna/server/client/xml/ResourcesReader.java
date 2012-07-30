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

package uk.org.taverna.server.client.xml;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.io.IOUtils;

import uk.org.taverna.server.client.connection.Connection;
import uk.org.taverna.server.client.connection.URIUtils;
import uk.org.taverna.server.client.connection.UserCredentials;
import uk.org.taverna.server.client.xml.Resources.Label;
import uk.org.taverna.server.client.xml.rest.ListenerDescription;
import uk.org.taverna.server.client.xml.rest.Location;
import uk.org.taverna.server.client.xml.rest.PolicyDescription;
import uk.org.taverna.server.client.xml.rest.PropertyDescription;
import uk.org.taverna.server.client.xml.rest.RunDescription;
import uk.org.taverna.server.client.xml.rest.RunList;
import uk.org.taverna.server.client.xml.rest.ServerDescription;
import uk.org.taverna.server.client.xml.rest.TavernaRun;
import uk.org.taverna.server.client.xml.rest.TavernaRunInputs;

public final class ResourcesReader {

	private final Connection connection;

	public ResourcesReader(Connection connection) {
		this.connection = connection;
	}

	public Object read(URI uri, UserCredentials credentials) {
		Object resources = null;
		InputStream is = connection.readStream(uri, "application/xml",
				credentials);

		try {
			JAXBContext context = JAXBContext
					.newInstance("uk.org.taverna.server.client.xml.rest");
			Unmarshaller unmarshaller = context.createUnmarshaller();
			resources = unmarshaller.unmarshal(is);
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			IOUtils.closeQuietly(is);
		}

		return resources;
	}

	public Object read(URI uri) {
		return read(uri, null);
	}

	public ServerResources readServerResources(URI uri) {
		Map<Label, URI> links = new HashMap<Label, URI>();

		// Read server top-level description.
		ServerDescription sd = (ServerDescription) read(uri);
		String version = sd.getServerVersion();
		String revision = sd.getServerRevision();
		String timestamp = sd.getServerBuildTimestamp();
		links.put(Label.RUNS, sd.getRuns().getHref());
		links.put(Label.POLICY, sd.getPolicy().getHref());
		links.put(Label.FEED, sd.getFeed().getHref());

		// Read policy description and add links to server's set.
		PolicyDescription pd = (PolicyDescription) read(links.get(Label.POLICY));
		links.put(Label.RUNLIMIT, pd.getRunLimit().getHref());
		links.put(Label.PERMITTED_WORKFLOWS, pd.getPermittedWorkflows()
				.getHref());
		links.put(Label.PERMITTED_LISTENERS, pd.getPermittedListenerTypes()
				.getHref());
		links.put(Label.ENABLED_NOTIFICATIONS, pd
				.getEnabledNotificationFabrics().getHref());

		return new ServerResources(links, version, revision, timestamp);
	}

	public Map<String, URI> readRunList(URI uri, UserCredentials credentials) {
		RunList runList = (RunList) read(uri, credentials);
		List<TavernaRun> trs = runList.getRun();

		Map<String, URI> runs = new HashMap<String, URI>(trs.size());

		for (TavernaRun tr : trs) {
			runs.put(tr.getValue(), tr.getHref());
		}

		return runs;
	}

	public RunResources readRunResources(URI uri, UserCredentials credentials) {
		Map<Label, URI> links = new HashMap<Label, URI>();

		// Read run top-level description.
		RunDescription rd = (RunDescription) read(uri, credentials);
		String owner = rd.getOwner();
		links.put(Label.WORKFLOW, rd.getCreationWorkflow().getHref());
		links.put(Label.CREATE_TIME, rd.getCreateTime().getHref());
		links.put(Label.START_TIME, rd.getStartTime().getHref());
		links.put(Label.FINISH_TIME, rd.getFinishTime().getHref());
		links.put(Label.STATUS, rd.getStatus().getHref());
		links.put(Label.INPUT, rd.getInputs().getHref());
		links.put(Label.OUTPUT, rd.getOutput().getHref());
		links.put(Label.WDIR, rd.getWorkingDirectory().getHref());
		links.put(Label.EXPIRY, rd.getExpiry().getHref());

		// Read the inputs description.
		JAXBElement<?> root = (JAXBElement<?>) read(links.get(Label.INPUT),
				credentials);
		TavernaRunInputs tri = (TavernaRunInputs) root.getValue();
		links.put(Label.BACLAVA, tri.getBaclava().getHref());
		links.put(Label.EXPECTED_INPUTS, tri.getExpected().getHref());

		// Read the special IO listeners - this is kind of hard-coded for now.
		for (Location loc : rd.getListeners().getListener()) {
			URI u = loc.getHref();
			if (URIUtils.extractFinalPathComponent(u).equalsIgnoreCase("io")) {
				root = (JAXBElement<?>) read(u, credentials);
				ListenerDescription ld = (ListenerDescription) root.getValue();

				for (PropertyDescription pd : ld.getProperties().getProperty()) {
					if (pd.getName().equalsIgnoreCase("stdout")) {
						links.put(Label.STDOUT, pd.getHref());
					} else if (pd.getName().equalsIgnoreCase("stderr")) {
						links.put(Label.STDERR, pd.getHref());
					} else if (pd.getName().equalsIgnoreCase("exitcode")) {
						links.put(Label.EXITCODE, pd.getHref());
					}
				}
			}
		}

		return new RunResources(links, owner);
	}
}

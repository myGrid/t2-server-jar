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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.sf.practicalxml.ParseUtil;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.IntRange;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import uk.org.taverna.server.client.connection.Connection;
import uk.org.taverna.server.client.connection.ConnectionFactory;
import uk.org.taverna.server.client.connection.UserCredentials;
import uk.org.taverna.server.client.connection.params.ConnectionParams;

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
public final class Server {
	private final Connection connection;

	private final URI uri;
	private final float version;
	private final Map<String, Run> runs;

	private final Map<String, String> links;

	private final XmlUtils xmlUtils;

	/**
	 * 
	 * @param uri
	 * @param params
	 */
	public Server(URI uri, ConnectionParams params) {
		// strip out username and password if present in server URI
		String userInfo = uri.getUserInfo();
		if (userInfo != null) {
			try {
				this.uri = new URI(uri.getScheme(), null, uri.getHost(),
						uri.getPort(), uri.getPath(), null, null);
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("Bad URI passed in: " + uri);
			}
		} else {
			this.uri = uri;
		}

		connection = ConnectionFactory.getConnection(this.uri, params);
		xmlUtils = XmlUtils.getInstance();

		// add a slash to the end of this address to work around this bug:
		// http://www.mygrid.org.uk/dev/issues/browse/TAVSERV-113
		String restPath = this.uri.toASCIIString() + "/rest/";
		Document doc = ParseUtil.parse(new String(connection.getAttribute(
				restPath, null)));
		version = getServerVersion(doc);
		links = getServerDescription(doc);

		// System.out.println("u: " + this.uri);
		// System.out.println("v: " + version);
		// for (String s : links.keySet()) {
		// System.out.println(s + ": " + links.get(s));
		// }

		// initialise run list
		runs = new HashMap<String, Run>();
		// getRunsFromServer();
	}

	/**
	 * 
	 * @param uri
	 */
	public Server(URI uri) {
		this(uri, null);
	}

	/**
	 * Get the version of the remote Taverna Server instance.
	 * 
	 * @return the Taverna Server version.
	 */
	public float getVersion() {
		return version;
	}

	/**
	 * Get the Run instance hosted by this Server by its id.
	 * 
	 * @param id
	 *            The id of the Run instance to get.
	 * @return the Run instance.
	 */
	public Run getRun(String id, UserCredentials credentials) {
		return getRunsFromServer(credentials).get(id);
	}

	/**
	 * Get all the Run instances hosted on this server.
	 * 
	 * @return all the Run instances hosted on this server.
	 */
	public Collection<Run> getRuns(UserCredentials credentials) {
		return getRunsFromServer(credentials).values();
	}

	/**
	 * Delete a Run from the server.
	 * 
	 * @param id
	 *            The id of the run to delete.
	 */
	public void deleteRun(String id, UserCredentials credentials) {
		try {
			connection.delete(links.get("runs") + "/" + id, credentials);
		} catch (AccessForbiddenException e) {
			if (getRunsFromServer(credentials).containsKey(id)) {
				throw e;
			} else {
				throw new RunNotFoundException(id);
			}
		}
	}

	/**
	 * Delete a Run from the server.
	 * 
	 * @param run
	 *            The Run instance to delete.
	 */
	public void deleteRun(Run run, UserCredentials credentials) {
		deleteRun(run.getIdentifier(), credentials);
	}

	/**
	 * Delete all runs on this server instance.
	 */
	public void deleteAllRuns(UserCredentials credentials) {
		for (Run run : getRuns(credentials)) {
			run.delete();
		}
	}

	private Map<String, Run> getRunsFromServer(UserCredentials credentials) {
		String runList = new String(connection.getAttribute(links.get("runs"),
				credentials));
		Document doc = ParseUtil.parse(runList);

		// add new runs, but keep a list of the new
		// ids so we can remove the stale ones below.
		String id;
		ArrayList<String> ids = new ArrayList<String>();
		for (Element e : xmlUtils.evalXPath(doc, "//nsr:run")) {
			id = e.getTextContent().trim();
			ids.add(id);
			if (!runs.containsKey(id)) {
				runs.put(id, new Run(this, id, credentials));
			}
		}

		// any ids in the runs list that aren't in the list we've
		// just got from the server are dead and can be removed.
		if (runs.size() > ids.size()) {
			for (String i : runs.keySet()) {
				if (!ids.contains(i)) {
					runs.remove(i);
				}
			}
		}

		assert (runs.size() == ids.size());

		return runs;
	}

	private float getServerVersion(Document doc) {
		String version = xmlUtils.evalXPath(doc, "/nsr:serverDescription",
				"nss:serverVersion");

		if (version.equalsIgnoreCase("")) {
			return 1.0f;
		} else {
			return Float.parseFloat(version.substring(0, 3));
		}
	}

	private Map<String, String> getServerDescription(Document doc) {
		HashMap<String, String> links = new HashMap<String, String>();

		links.put("runs", xmlUtils.evalXPath(doc, "//nsr:runs", "xlink:href"));

		if (version > 1.0) {
			String policy = xmlUtils.evalXPath(doc, "//nsr:policy",
					"xlink:href");

			links.put("policy", policy);
			doc = ParseUtil.parse(new String(connection.getAttribute(policy,
					null)));

			links.put("permlisteners", xmlUtils.evalXPath(doc,
					"//nsr:permittedListenerTypes", "xlink:href"));

			links.put("notifications", xmlUtils.evalXPath(doc,
					"//nsr:enabledNotificationFabrics", "xlink:href"));
		} else {
			links.put("permlisteners", xmlUtils.evalXPath(doc,
					"//nsr:permittedListeners", "xlink:href"));
		}

		links.put("runlimit",
				xmlUtils.evalXPath(doc, "//nsr:runLimit", "xlink:href"));
		links.put("permworkflows", xmlUtils.evalXPath(doc,
				"//nsr:permittedWorkflows", "xlink:href"));

		return links;
	}

	/**
	 * Connect to a Taverna Server.
	 * 
	 * @param uri
	 *            The address of the server to connect to in the form
	 *            http://server:port/location
	 * @return a Server instance representing the connection to the specified
	 *         Taverna Server.
	 * @throws URISyntaxException
	 *             if the provided URI is badly formed.
	 */
	@Deprecated
	public static Server connect(String uri) throws URISyntaxException {
		return new Server(new URI(uri));
	}

	/**
	 * Connect to a Taverna Server.
	 * 
	 * @param uri
	 *            The URI of the server to connect to.
	 * @return a Server instance representing the connection to the specified
	 *         Taverna Server.
	 */
	@Deprecated
	public static Server connect(URI uri) {
		return new Server(uri);
	}

	/**
	 * Get the URI of this server instance.
	 * 
	 * @return the URI of this server instance.
	 */
	public URI getUri() {
		return uri;
	}

	/**
	 * Get the URI of this server instance as a String.
	 * 
	 * @return the URI of this server instance as a String.
	 */
	public String getStringUri() {
		return uri.toASCIIString();
	}

	/**
	 * Get the maximum number of run that this server can host concurrently.
	 * 
	 * @return the maximum number of run that this server can host concurrently.
	 */
	public int getRunLimit(UserCredentials credentials) {
		return Integer.parseInt(new String(connection.getAttribute(
				links.get("runlimit"), credentials)).trim());
	}

	/**
	 * Initialize a Run on this server instance.
	 * 
	 * @param workflow
	 *            the workflow to be run.
	 * @return the id of the new run as returned by the server.
	 */
	String initializeRun(byte[] workflow, UserCredentials credentials) {
		String id = null;
		String location = connection.upload(links.get("runs"),
				xmlUtils.buildXMLFragment("workflow", new String(workflow)),
				"application/xml", credentials);

		if (location != null) {
			id = location.substring(location.lastIndexOf("/") + 1);
		}

		return id;
	}

	/**
	 * Create a new Run on this server with the supplied workflow.
	 * 
	 * @param workflow
	 *            the workflow to be run.
	 * @return a new Run instance.
	 */
	public Run createRun(byte[] workflow, UserCredentials credentials) {
		return Run.create(this, workflow, credentials);
	}

	/**
	 * Create a new Run on this server with the supplied workflow file.
	 * 
	 * @param workflow
	 *            the workflow file to be run.
	 * @return a new Run instance.
	 */
	public Run createRun(File workflow, UserCredentials credentials)
			throws IOException {
		return Run.create(this, workflow, credentials);
	}

	/**
	 * Read attribute data from a run.
	 * 
	 * @param id
	 *            the id of the run.
	 * @param uri
	 *            the full URI of the attribute to get.
	 * @param type
	 *            the mime type of the attribute being retrieved.
	 * @return the data associated with the attribute.
	 */
	public byte[] getRunData(String id, String uri, String type,
			UserCredentials credentials) {
		return getRunData(id, uri, type, null, credentials);
	}

	/**
	 * Read attribute data from a run.
	 * 
	 * @param id
	 *            the id of the run.
	 * @param uri
	 *            the full URI of the attribute to get.
	 * @param type
	 *            the mime type of the attribute being retrieved.
	 * @param range
	 * @param credentials
	 * @return the data associated with the attribute.
	 */
	public byte[] getRunData(String id, String uri, String type,
			IntRange range, UserCredentials credentials) {
		try {
			return connection.getAttribute(uri, type, range, credentials);
		} catch (AttributeNotFoundException e) {
			if (getRunsFromServer(credentials).containsKey(id)) {
				throw e;
			} else {
				throw new RunNotFoundException(id);
			}
		}
	}

	/**
	 * Read attribute data from a run.
	 * 
	 * @param run
	 *            the Run instance.
	 * @param uri
	 *            the full URI of the attribute to get.
	 * @param type
	 *            the mime type of the attribute being retrieved.
	 * @return the data associated with the attribute.
	 */
	public byte[] getRunData(Run run, String uri, String type,
			UserCredentials credentials) {
		return getRunData(run.getIdentifier(), uri, type, credentials);
	}

	/**
	 * 
	 * @param run
	 * @param uri
	 * @param type
	 * @param range
	 * @param credentials
	 * @return
	 */
	public byte[] getRunData(Run run, String uri, String type, IntRange range,
			UserCredentials credentials) {
		return getRunData(run.getIdentifier(), uri, type, range, credentials);
	}

	/**
	 * Read an attribute, of a specific type, of a run.
	 * 
	 * @param id
	 *            the id of the run.
	 * @param uri
	 *            the full URI of the attribute to get.
	 * @param type
	 *            the mime type of the attribute being retrieved.
	 * @return the attribute as a String.
	 */
	public String getRunAttribute(String id, String uri, String type,
			UserCredentials credentials) {
		return new String(getRunData(id, uri, type, credentials));
	}

	/**
	 * Read an attribute, of a specific type, of a run.
	 * 
	 * @param run
	 *            the Run instance.
	 * @param uri
	 *            the full URI of the attribute to get.
	 * @param type
	 *            the mime type of the attribute being retrieved.
	 * @return the attribute as a String.
	 */
	public String getRunAttribute(Run run, String uri, String type,
			UserCredentials credentials) {
		return new String(getRunData(run.getIdentifier(), uri, type, credentials));
	}

	/**
	 * Set a run's attribute to a new value.
	 * 
	 * @param id
	 *            the id of the run.
	 * @param uri
	 *            the full URI of the attribute to set.
	 * @param value
	 *            the new value of the attribute.
	 * @param type
	 *            the mime type of the attribute.
	 * @param credentials
	 *            the user credentials to use for authorization.
	 */
	public void setRunAttribute(String id, String uri, String value,
			String type, UserCredentials credentials) {
		try {
			connection.setAttribute(uri, value, type, credentials);
		} catch (AttributeNotFoundException e) {
			if (getRunsFromServer(credentials).containsKey(id)) {
				throw e;
			} else {
				throw new RunNotFoundException(id);
			}
		}
	}

	/**
	 * Set a run's attribute to a new value.
	 * 
	 * @param run
	 *            the Run instance.
	 * @param uri
	 *            the full URI of the attribute to set.
	 * @param value
	 *            the new value of the attribute.
	 * @param type
	 *            the mime type of the attribute.
	 * @param credentials
	 *            the user credentials to use for authorization.
	 */
	public void setRunAttribute(Run run, String uri, String value, String type,
			UserCredentials credentials) {
		setRunAttribute(run.getIdentifier(), uri, value, type, credentials);
	}

	/*
	 * This is a workaround until the connection stuff works with paths as
	 * opposed to full URIs.
	 */
	String getRunDescription(Run run, UserCredentials credentials) {
		return getRunAttribute(run,
				links.get("runs") + "/" + run.getIdentifier(),
				"application/xml", credentials);
	}

	/**
	 * Upload a file to the server for use by a run.
	 * 
	 * @param id
	 *            the id of the run to upload to.
	 * @param file
	 *            the file to upload.
	 * @param uploadLocation
	 *            the location to upload to. This should be the full URI.
	 * @param rename
	 *            optionally rename the file at the remote location. Pass null
	 *            or the empty string to ignore.
	 * @return the name of the file on the remote server. This will be unchanged
	 *         unless rename was used.
	 * @throws IOException
	 * @see #uploadRunFile(id, File, String)
	 * @see #uploadRunFile(Run, File, String, String)
	 * @see #uploadRunFile(Run, File, String)
	 */
	public String uploadRunFile(String id, File file, String uploadLocation,
			String rename, UserCredentials credentials) throws IOException {

		if (rename == null || rename.equals("")) {
			rename = file.getName();
		}

		byte[] data = FileUtils.readFileToByteArray(file);
		String contents = Base64.encodeBase64String(data);

		connection.upload(uploadLocation,
				xmlUtils.buildXMLFragment("upload", rename, contents),
				"application/xml", credentials);

		return rename;
	}

	/**
	 * Upload a file to the server for use by a run.
	 * 
	 * @param id
	 *            the id of the run to upload to.
	 * @param file
	 *            the file to upload.
	 * @param uploadLocation
	 *            the location to upload to. This should be the full URI.
	 * @return the name of the file on the remote server. This will be unchanged
	 *         unless rename was used.
	 * @throws IOException
	 * @see #uploadRunFile(id, File, String, String)
	 * @see #uploadRunFile(Run, File, String, String)
	 * @see #uploadRunFile(Run, File, String)
	 */
	public String uploadRunFile(String id, File file, String uploadLocation,
			UserCredentials credentials) throws IOException {
		return uploadRunFile(id, file, uploadLocation, null, credentials);
	}

	/**
	 * Upload a file to the server for use by a run.
	 * 
	 * @param run
	 *            the Run instance to upload to.
	 * @param file
	 *            the file to upload.
	 * @param uploadLocation
	 *            the location to upload to. This should be the full URI.
	 * @param rename
	 *            optionally rename the file at the remote location. Pass null
	 *            or the empty string to ignore.
	 * @return the name of the file on the remote server. This will be unchanged
	 *         unless rename was used.
	 * @throws IOException
	 * @see #uploadRunFile(Run, File, String)
	 * @see #uploadRunFile(id, File, String)
	 * @see #uploadRunFile(id, File, String, String)
	 */
	public String uploadRunFile(Run run, File file, String uploadLocation,
			String rename, UserCredentials credentials) throws IOException {
		return uploadRunFile(run.getIdentifier(), file, uploadLocation, rename,
				credentials);
	}

	/**
	 * Upload a file to the server for use by a run.
	 * 
	 * @param run
	 *            the Run instance to upload to.
	 * @param file
	 *            the file to upload.
	 * @param uploadLocation
	 *            the location to upload to. This should be the full URI.
	 * @return the name of the file on the remote server. This will be unchanged
	 *         unless rename was used.
	 * @throws IOException
	 * @see #uploadRunFile(Run, File, String, String)
	 * @see #uploadRunFile(id, File, String)
	 * @see #uploadRunFile(id, File, String, String)
	 */
	public String uploadRunFile(Run run, File file, String uploadLocation,
			UserCredentials credentials) throws IOException {
		return uploadRunFile(run.getIdentifier(), file, uploadLocation, null,
				credentials);
	}

	void makeRunDir(String id, String root, String name,
			UserCredentials credentials) throws IOException {
		if (name.contains("/")) {
			throw new AccessForbiddenException(
					"creation of subdirectories directly (" + name + ")");
		}

		try {
			connection.upload(root, xmlUtils.buildXMLFragment("mkdir", name),
					"application/xml", credentials);
		} catch (AttributeNotFoundException e) {
			if (getRunsFromServer(credentials).containsKey(id)) {
				throw e;
			} else {
				throw new RunNotFoundException(id);
			}
		}
	}

	void makeRunDir(Run run, String root, String name,
			UserCredentials credentials) throws IOException {
		makeRunDir(run.getIdentifier(), root, name, credentials);
	}
}

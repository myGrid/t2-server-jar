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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import net.sf.practicalxml.ParseUtil;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.math.LongRange;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import uk.org.taverna.server.client.connection.URIUtils;
import uk.org.taverna.server.client.connection.UserCredentials;
import uk.org.taverna.server.client.xml.Resources.Label;
import uk.org.taverna.server.client.xml.ResourcesWriter;
import uk.org.taverna.server.client.xml.RunResources;

/**
 * The Run class represents a workflow run on a Taverna Server instance. It is
 * created by supplying a Server instance on which to create it and a workflow
 * to be run.
 * 
 * @author Robert Haines
 */
public final class Run {

	/*
	 * Internal names to use for storing baclava input and output in files on
	 * the server.
	 */
	private static final String BACLAVA_IN_FILE = "in.baclava";
	private static final String BACLAVA_OUT_FILE = "out.baclava";

	private final URI uri;
	private final Server server;
	private final String id;
	private byte[] workflow;
	private boolean baclavaIn;
	private boolean baclavaOut;

	private RunResources resources;

	private final XmlUtils xmlUtils;

	private final UserCredentials credentials;

	// Ports
	private Map<String, InputPort> inputPorts = null;
	private Map<String, OutputPort> outputPorts = null;

	/*
	 * Create a Run instance. This will already have been created on the remote
	 * server.
	 */
	private Run(URI uri, Server server, byte[] workflow,
			UserCredentials credentials) {
		this.uri = uri;
		this.server = server;
		this.id = URIUtils.extractFinalPathComponent(uri);
		this.workflow = workflow;
		this.baclavaIn = false;
		this.baclavaOut = false;

		xmlUtils = XmlUtils.getInstance();

		this.credentials = credentials;
		resources = null;
	}

	/*
	 * Internal constructor for other classes within the package to create
	 * "lightweight" runs. Used when listing runs, etc.
	 */
	Run(URI uri, Server server, UserCredentials credentials) {
		this(uri, server, null, credentials);
	}

	/**
	 * 
	 * @param server
	 * @param workflow
	 * @param credentials
	 * @return
	 */
	public static Run create(Server server, byte[] workflow,
			UserCredentials credentials) {
		URI uri = server.initializeRun(workflow, credentials);

		return new Run(uri, server, workflow, credentials);
	}

	/**
	 * 
	 * @param server
	 * @param workflow
	 * @param credentials
	 * @return
	 * @throws IOException
	 */
	public static Run create(Server server, File workflow,
			UserCredentials credentials) throws IOException {
		return create(server, FileUtils.readFileToByteArray(workflow),
				credentials);
	}

	/**
	 * 
	 * @return
	 */
	public URI getURI() {
		return uri;
	}

	/**
	 * 
	 * @return
	 */
	public Server getServer() {
		return server;
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, InputPort> getInputPorts() {
		if (inputPorts == null) {
			inputPorts = getInputPortInfo();
		}

		return inputPorts;
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public InputPort getInputPort(String name) {
		return getInputPorts().get(name);
	}

	/**
	 * 
	 * @return
	 */
	public Map<String, OutputPort> getOutputPorts() {
		if (outputPorts == null) {
			outputPorts = getOutputPortInfo();
		}

		return outputPorts;
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	public OutputPort getOutputPort(String name) {
		return getOutputPorts().get(name);
	}

	/**
	 * Upload data to a file in this Run instance's workspace on the server.
	 * 
	 * @param data
	 *            The data to upload.
	 * @param remoteName
	 *            The name of the file to save the data in on the server.
	 * @param remoteDirectory
	 *            The directory within the workspace in which to save the data.
	 *            This directory must already exist.
	 */
	public void uploadData(byte[] data, String remoteName,
			String remoteDirectory) {
		URI uploadLocation = getLink(Label.WDIR);
		if (remoteDirectory != null) {
			uploadLocation = URIUtils
					.appendToPath(uploadLocation, remoteDirectory);
		}

		server.uploadData(uploadLocation, data, remoteName, credentials);
	}

	/**
	 * Upload data to a file in this Run instance's workspace on the server.
	 * 
	 * @param data
	 *            The data to upload.
	 * @param remoteName
	 *            The name of the file to save the data in on the server.
	 */
	public void uploadData(byte[] data, String remoteName) {
		uploadData(data, remoteName, null);
	}

	/**
	 * Upload a file to this Run instance's workspace on the server.
	 * 
	 * @param file
	 *            The file to upload.
	 * @param remoteDirectory
	 *            The directory within the workspace to upload the file to.
	 * @param rename
	 *            The name to use for the file when saving it in the workspace.
	 * @return the name of the file as used on the server.
	 * @throws IOException
	 */
	public String uploadFile(File file, String remoteDirectory, String rename)
			throws IOException {
		if (rename == null || rename.equals("")) {
			rename = file.getName();
		}

		byte[] data = FileUtils.readFileToByteArray(file);
		uploadData(data, rename, remoteDirectory);

		return rename;
	}

	/**
	 * Upload a file to this Run instance's workspace on the server.
	 * 
	 * @param file
	 *            The file to upload.
	 * @param remoteDirectory
	 *            The directory within the workspace to upload the file to.
	 * @return the name of the file as used on the server.
	 * @throws IOException
	 */
	public String uploadFile(File file, String remoteDirectory)
			throws IOException {
		return uploadFile(file, remoteDirectory, null);
	}

	/**
	 * Upload a file to this Run instance's workspace on the server.
	 * 
	 * @param file
	 *            The file to upload.
	 * @return the name of the file as used on the server.
	 * @throws IOException
	 */
	public String uploadFile(File file) throws IOException {
		return uploadFile(file, null, null);
	}

	/**
	 * Upload baclava data to specify all input port value.
	 * 
	 * @param data
	 *            The data to upload.
	 */
	public void setBaclavaInput(byte[] data) {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			uploadData(data, BACLAVA_IN_FILE);
			server.setRunAttribute(this, getLink(Label.BACLAVA),
					BACLAVA_IN_FILE, "text/plain", credentials);

			baclavaIn = true;
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	/**
	 * Upload a baclava file to specify all input port values.
	 * 
	 * @param file
	 *            The file to upload.
	 * @throws IOException
	 */
	public void setBaclavaInput(File file) throws IOException {
		byte[] data = FileUtils.readFileToByteArray(file);
		setBaclavaInput(data);
	}

	/**
	 * Is this run using baclava to set all its input ports?
	 * 
	 * @return true if yes, false if not.
	 */
	public boolean isBaclavaInput() {
		// if baclavaIn is true then we know that is correct, else we check.
		if (baclavaIn) {
			return true;
		} else {
			String test = server.getRunAttribute(this, getLink(Label.BACLAVA),
					"text/plain", credentials);

			// if we get back the baclava input file name we are using it.
			if (test.equals(BACLAVA_IN_FILE)) {
				baclavaIn = true;
			}

			return baclavaIn;
		}
	}

	/**
	 * Is this run using baclava to return all its output port data?
	 * 
	 * @return true if yes, false if not.
	 */
	public boolean isBaclavaOutput() {
		// if baclavaOut is true then we know that is correct, else we check.
		if (baclavaOut) {
			return true;
		} else {
			String test = server.getRunAttribute(this, getLink(Label.OUTPUT),
					"text/plain", credentials);

			// if we get back the baclava output file name we are using it.
			if (test.equals(BACLAVA_OUT_FILE)) {
				baclavaOut = true;
			}

			return baclavaOut;
		}
	}

	/**
	 * Set the server to return outputs for this Run in baclava format. This
	 * must be set before the Run is started.
	 */
	public void requestBaclavaOutput() {
		// don't try and request it again!
		if (baclavaOut) {
			return;
		}

		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			server.setRunAttribute(this, getLink(Label.OUTPUT),
					BACLAVA_OUT_FILE, "text/plain", credentials);

			baclavaOut = true;
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	/**
	 * Get the outputs of this Run as a baclava formatted document. The Run must
	 * have been set to output in baclava format before it is started.
	 * 
	 * @return The baclava formatted document contents as a byte array.
	 * @see #requestBaclavaOutput()
	 * @see #getBaclavaOutputStream()
	 * @see #writeBaclavaOutputToFile(File)
	 */
	public byte[] getBaclavaOutput() {
		RunStatus rs = getStatus();
		if (rs == RunStatus.FINISHED) {
			URI baclavaLink = URIUtils.appendToPath(getLink(Label.WDIR),
					BACLAVA_OUT_FILE);
			if (!baclavaOut) {
				throw new AttributeNotFoundException(baclavaLink);
			}

			return server.getRunData(this, baclavaLink,
					"application/octet-stream", credentials);
		} else {
			throw new RunStateException(rs, RunStatus.FINISHED);
		}
	}

	/**
	 * Get an input stream that can be used to stream the baclava output data of
	 * this run. The Run must have been set to output in baclava format before
	 * it is started.
	 * 
	 * <b>Note:</b> You are responsible for closing the stream once you have
	 * finished with it. Not doing so may prevent further use of the underlying
	 * network connection.
	 * 
	 * @return The stream to read the baclava data from.
	 * @see #getBaclavaOutput()
	 * @see #writeBaclavaOutputToFile(File)
	 * @see #requestBaclavaOutput()
	 */
	public InputStream getBaclavaOutputStream() {
		RunStatus rs = getStatus();
		if (rs == RunStatus.FINISHED) {
			URI baclavaLink = URIUtils.appendToPath(getLink(Label.WDIR),
					BACLAVA_OUT_FILE);
			if (!baclavaOut) {
				throw new AttributeNotFoundException(baclavaLink);
			}

			return server.getDataStream(baclavaLink,
					"application/octet-stream", null, credentials);
		} else {
			throw new RunStateException(rs, RunStatus.FINISHED);
		}
	}

	/**
	 * Writes the baclava output data of this run directly to a file. The data
	 * is not loaded into memory, it is streamed directly to the file. The file
	 * is created if it does not already exist and will overwrite existing data
	 * if it does.
	 * 
	 * The Run must have been set to output in baclava format before it is
	 * started.
	 * 
	 * @param file
	 *            the file to write to.
	 * @throws FileNotFoundException
	 *             if the file exists but is a directory rather than a regular
	 *             file, does not exist but cannot be created, or cannot be
	 *             opened for any other reason.
	 * @throws IOException
	 *             if there is any I/O error.
	 * @see #getBaclavaOutput()
	 * @see #getBaclavaOutputStream()
	 * @see #requestBaclavaOutput()
	 */
	public void writeBaclavaOutputToFile(File file) throws IOException {
		writeStreamToFile(getBaclavaOutputStream(), file);
	}

	/**
	 * Get the id of this run.
	 * 
	 * @return the id of this run.
	 */
	public String getIdentifier() {
		return id;
	}

	/**
	 * Get the status of this Run.
	 * 
	 * @return the status of this Run.
	 */
	public RunStatus getStatus() {
		return RunStatus.state(server.getRunAttribute(this,
				getLink(Label.STATUS), "text/plain", credentials));
	}

	/**
	 * Is this Run initialized?
	 * 
	 * @return true if the Run is initialized, false otherwise.
	 */
	public boolean isInitialized() {
		return getStatus() == RunStatus.INITIALIZED;
	}

	/**
	 * Is this Run running?
	 * 
	 * @return true if the Run is running, false otherwise.
	 */
	public boolean isRunning() {
		return getStatus() == RunStatus.RUNNING;
	}

	/**
	 * Is this Run finished?
	 * 
	 * @return true if the Run is finished, false otherwise.
	 */
	public boolean isFinished() {
		return getStatus() == RunStatus.FINISHED;
	}

	/**
	 * Start this Run running on the server. The Run must not be already
	 * running, or finished.
	 * 
	 * @throws IOException
	 */
	public void start() throws IOException {
		RunStatus rs = getStatus();
		if (rs != RunStatus.INITIALIZED) {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}

		// set all the inputs
		if (!isBaclavaInput()) {
			setAllInputs();
		}

		server.setRunAttribute(this, getLink(Label.STATUS),
				RunStatus.RUNNING.status(), "text/plain", credentials);
	}

	/**
	 * Get the workflow of this Run as a String.
	 * 
	 * @return the workflow of this Run as a String.
	 */
	public byte[] getWorkflow() {
		if (workflow == null) {
			workflow = server.getRunData(this, getLink(Label.WORKFLOW),
					"application/xml", credentials);
		}

		return workflow;
	}

	/**
	 * Get the expiry time of this Run as a Date object.
	 * 
	 * @return the expiry time of this Run as a Date object.
	 */
	public Date getExpiry() {
		return getTime(Label.EXPIRY);
	}

	/**
	 * Set the expiry time of this Run.
	 * 
	 * @param time
	 *            the new expiry time of this Run.
	 */
	public void setExpiry(Date time) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(time);
		String expiry = DatatypeConverter.printDateTime(cal);
		server.setRunAttribute(this, getLink(Label.EXPIRY), expiry,
				"text/plain", credentials);
	}

	/**
	 * Delete this Run.
	 */
	public void delete() {
		server.deleteRun(this, credentials);
	}

	/**
	 * Get the return code of the underlying Taverna Server process. A zero
	 * value indicates success.
	 * 
	 * @return the return code of the underlying Taverna Server process.
	 */
	public int getExitCode() {
		return new Integer(server.getRunAttribute(this,
				getLink(Label.EXITCODE), "text/plain", credentials));
	}

	/**
	 * Get the console output of the underlying Taverna Server process.
	 * 
	 * @return the console output of the underlying Taverna Server process.
	 */
	public String getConsoleOutput() {
		return server.getRunAttribute(this, getLink(Label.STDOUT),
				"text/plain", credentials);
	}

	/**
	 * Get the console errors of the underlying Taverna Server process.
	 * 
	 * @return the console errors of the underlying Taverna Server process.
	 */
	public String getConsoleError() {
		return server.getRunAttribute(this, getLink(Label.STDERR),
				"text/plain", credentials);
	}

	/**
	 * Get the time that this Run was created as a Date object.
	 * 
	 * @return the time that this Run was created.
	 */
	public Date getCreateTime() {
		return getTime(Label.CREATE_TIME);
	}

	/**
	 * Get the time that this Run was started as a Date object.
	 * 
	 * @return the time that this Run was started.
	 */
	public Date getStartTime() {
		return getTime(Label.START_TIME);
	}

	/**
	 * Get the time that this Run finished as a Date object.
	 * 
	 * @return the time that this Run finished.
	 */
	public Date getFinishTime() {
		return getTime(Label.FINISH_TIME);
	}

	private Date getTime(Label time) {
		String dateTime = server.getRunAttribute(this, getLink(time),
				"text/plain", credentials);
		Calendar cal = DatatypeConverter.parseDateTime(dateTime);

		return cal.getTime();
	}

	/**
	 * Get an input stream that can be used to stream all the output data of
	 * this run in zip format.
	 * 
	 * <b>Note:</b> You are responsible for closing the stream once you have
	 * finished with it. Not doing so may prevent further use of the underlying
	 * network connection.
	 * 
	 * @return The stream to read the zip data from.
	 * @see #writeOutputToZipFile(File)
	 */
	public InputStream getOutputZipStream() {
		RunStatus rs = getStatus();
		if (rs == RunStatus.FINISHED) {
			URI uri = URIUtils.appendToPath(getLink(Label.WDIR), "out");

			return server.getDataStream(uri, "application/zip", null,
					credentials);
		} else {
			throw new RunStateException(rs, RunStatus.FINISHED);
		}
	}

	/**
	 * Writes all the output data of this run directly to a file in zip format.
	 * The data is not loaded into memory, it is streamed directly to the file.
	 * The file is created if it does not already exist and will overwrite
	 * existing data if it does.
	 * 
	 * @param file
	 *            the file to write to.
	 * @throws FileNotFoundException
	 *             if the file exists but is a directory rather than a regular
	 *             file, does not exist but cannot be created, or cannot be
	 *             opened for any other reason.
	 * @throws IOException
	 *             if there is any I/O error.
	 * @see #getOutputZipStream()
	 */
	public void writeOutputToZipFile(File file) throws IOException {
		writeStreamToFile(getOutputZipStream(), file);
	}

	/**
	 * Create a directory in the workspace of this Run.
	 * 
	 * @param dir
	 *            the name of the directory to create.
	 * @throws IOException
	 */
	public void mkdir(String dir) throws IOException {
		if (dir.contains("/")) {
			int lastSlash = dir.lastIndexOf("/");
			String leaf = dir.substring(lastSlash + 1, dir.length());
			String path = dir.substring(0, lastSlash);
			server.makeRunDir(this,
					URIUtils.appendToPath(getLink(Label.WDIR), path), leaf,
					credentials);
		} else {
			server.makeRunDir(this, getLink(Label.WDIR), dir, credentials);
		}
	}

	/*
	 * Set all the inputs on the server. The inputs must have been set prior to
	 * this call using the InputPort API or a runtime exception is thrown.
	 */
	private void setAllInputs() throws IOException {
		List<String> missingPorts = new ArrayList<String>();

		for (InputPort port : getInputPorts().values()) {
			if (!port.isSet()) {
				missingPorts.add(port.getName());
			}

			if (port.isFile()) {
				// If we're using a local file upload it first then set the
				// port to use a remote file.
				if (!port.isRemoteFile()) {
					String file = uploadFile(port.getFile());
					port.setRemoteFile(file);
				}

				setInputPort(port);
			} else {
				setInputPort(port);
			}
		}

		if (!missingPorts.isEmpty()) {
			throw new RunInputsNotSetException(id, missingPorts);
		}
	}

	private void setInputPort(InputPort port) {
		URI path = URIUtils.appendToPath(getLink(Label.INPUT),
				"/input/" + port.getName());
		byte[] value;

		if (port.isFile()) {
			value = ResourcesWriter.inputFile(port.getFile());
		} else {
			value = ResourcesWriter.inputValue(port.getValue());
		}

		server.setRunAttribute(this, path, value, "application/xml",
				credentials);
	}

	private RunResources getRunResources() {
		if (resources == null) {
			resources = server.getResourcesReader().readRunResources(uri,
					credentials);
		}

		return resources;
	}

	private Map<String, InputPort> getInputPortInfo() {
		Map<String, InputPort> ports = new HashMap<String, InputPort>();

		String portDesc = server.getRunAttribute(this,
				getLink(Label.EXPECTED_INPUTS), "application/xml", credentials);
		Document doc = ParseUtil.parse(portDesc);

		for (Element e : xmlUtils.evalXPath(doc, "//port:input")) {
			InputPort port = new InputPort(this, e);
			ports.put(port.getName(), port);
		}

		return ports;
	}

	private Map<String, OutputPort> getOutputPortInfo() {
		Map<String, OutputPort> ports = new HashMap<String, OutputPort>();

		String portDesc = server.getRunAttribute(this, getLink(Label.OUTPUT),
				"application/xml", credentials);
		Document doc = ParseUtil.parse(portDesc);

		for (Element e : xmlUtils.evalXPath(doc, "//port:output")) {
			OutputPort port = new OutputPort(this, e);
			ports.put(port.getName(), port);
		}

		return ports;
	}

	byte[] getOutputData(URI uri, LongRange range) {
		return server.getData(uri, "application/octet-stream", range,
				credentials);
	}

	InputStream getOutputDataStream(URI uri, LongRange range) {
		return server.getDataStream(uri, "application/octet-stream", range,
				credentials);
	}

	/*
	 * This method just abstracts out the basic stream-to-file code.
	 */
	void writeStreamToFile(InputStream stream, File file) throws IOException {
		OutputStream os = null;
		try {
			os = new FileOutputStream(file);
			IOUtils.copyLarge(stream, os);
		} finally {
			IOUtils.closeQuietly(stream);
			IOUtils.closeQuietly(os);
		}
	}

	private URI getLink(Label key) {
		return getRunResources().get(key);
	}
}

/*
 * Copyright (c) 2010, 2011 The University of Manchester, UK.
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import net.sf.practicalxml.ParseUtil;
import net.sf.practicalxml.XmlUtil;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * 
 * @author Robert Haines
 * 
 */
public final class Run {

	private final Server server;
	private final UUID uuid;
	private String workflow;
	private String baclavaOut;

	private final Map<String, String> links;

	private final XmlUtils xmlUtils;

	private Run(Server server, String workflow, UUID uuid) {
		this.server = server;
		this.uuid = uuid;
		this.workflow = workflow;
		this.baclavaOut = null;

		xmlUtils = XmlUtils.getInstance();

		links = getRunDescription();

		// for (String s : links.values()) {
		// System.out.println(s);
		// }
	}

	public Run(Server server, String workflow) {
		this(server, workflow, server.initializeRun(workflow));
	}

	Run(Server server, UUID uuid) {
		this(server, null, uuid);
	}

	public void setInput(String input, String value) {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			server.setRunInput(this, input, value);
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	public void setInputFile(String input, String filename) {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			server.setRunInputFile(this, input, filename);
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	public String uploadFile(File file, String remoteDirectory, String rename)
			throws IOException {
		String uploadLocation = links.get("wdir");
		uploadLocation += remoteDirectory != null ? "/" + remoteDirectory : "";
		return server.uploadRunFile(this, file, uploadLocation, rename);
	}

	public String uploadFile(File file) throws IOException {
		return uploadFile(file, null, null);
	}

	public void uploadInputFile(String input, File file,
			String remoteDirectory, String rename) throws IOException {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			String filename = uploadFile(file, remoteDirectory, rename);
			setInputFile(input, filename);
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	public void uploadBaclavaFile(File file) throws IOException {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			String filename = uploadFile(file);
			server.setRunAttribute(this, links.get("baclava"), filename);
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	public void setBaclavaOutput(String name) {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			this.baclavaOut = name;
			server.setRunAttribute(this, links.get("output"), baclavaOut);
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	public void setBaclavaOutput() {
		setBaclavaOutput("out.xml");
	}

	public String getBaclavaOutput() {
		RunStatus rs = getStatus();
		if (rs == RunStatus.FINISHED) {
			String baclavaLink = links.get("wdir") + "/" + baclavaOut;
			if (baclavaOut == null) {
				throw new AttributeNotFoundException(baclavaLink);
			}

			return server.getRunAttribute(this, baclavaLink,
					"application/octet-stream");
		} else {
			throw new RunStateException(rs, RunStatus.FINISHED);
		}
	}

	public UUID getUUID() {
		return uuid;
	}

	public RunStatus getStatus() {
		return RunStatus
				.state(server.getRunAttribute(this, links.get("status")));
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

	public void start() {
		RunStatus rs = getStatus();
		if (rs == RunStatus.INITIALIZED) {
			server.setRunAttribute(this, links.get("status"),
					RunStatus.RUNNING.status());
		} else {
			throw new RunStateException(rs, RunStatus.INITIALIZED);
		}
	}

	public String getWorkflow() {
		if (workflow == null) {
			workflow = server.getRunAttribute(this, links.get("workflow"));
		}

		return workflow;
	}

	public Date getExpiry() {
		return getTime("expiry");
	}

	public void setExpiry(Date time) {
		System.out.println(XmlUtil.formatXsdDatetime(time));
	}

	public void delete() {
		server.deleteRun(this);
	}

	String getInputsPath() {
		return links.get("inputs");
	}

	public List<String> getOutputPorts() {
		List<String> lists = new ArrayList<String>();
		List<String> items = new ArrayList<String>();

		ls_ports("out", lists, items);

		// return concatenated lists
		items.addAll(lists);
		return items;
	}

	public int getExitCode() {
		return new Integer(server.getRunAttribute(this, links.get("exitcode")));
	}

	public String getConsoleOutput() {
		return server.getRunAttribute(this, links.get("stdout"));
	}

	public String getConsoleError() {
		return server.getRunAttribute(this, links.get("stderr"));
	}

	public Date getCreateTime() {
		return getTime("createtime");
	}

	public Date getStartTime() {
		return getTime("starttime");
	}

	public Date getFinishTime() {
		return getTime("finishtime");
	}

	private Date getTime(String time) {
		return XmlUtil.parseXsdDatetime(server.getRunAttribute(this,
				links.get(time)));

	}

	public void mkdir(String dir) throws IOException {
		if (dir.contains("/")) {
			int lastSlash = dir.lastIndexOf("/");
			String leaf = dir.substring(lastSlash + 1, dir.length());
			String path = dir.substring(0, lastSlash);
			server.makeRunDir(this, links.get("wdir") + "/" + path, leaf);
		} else {
			server.makeRunDir(this, links.get("wdir"), dir);
		}
	}

	private Map<String, String> getRunDescription() {
		HashMap<String, String> links = new HashMap<String, String>();

		// parse out the simple stuff
		String description = server.getRunDescription(this);
		Document doc = ParseUtil.parse(description);

		links.put("expiry",
				xmlUtils.evalXPath(doc, "//nsr:expiry", "xlink:href"));
		links.put("workflow",
				xmlUtils.evalXPath(doc, "//nsr:creationWorkflow", "xlink:href"));
		links.put("status",
				xmlUtils.evalXPath(doc, "//nsr:status", "xlink:href"));
		links.put("createtime",
				xmlUtils.evalXPath(doc, "//nsr:createTime", "xlink:href"));
		links.put("starttime",
				xmlUtils.evalXPath(doc, "//nsr:startTime", "xlink:href"));
		links.put("finishtime",
				xmlUtils.evalXPath(doc, "//nsr:finishTime", "xlink:href"));
		links.put("wdir",
				xmlUtils.evalXPath(doc, "//nsr:workingDirectory", "xlink:href"));
		links.put("inputs",
				xmlUtils.evalXPath(doc, "//nsr:inputs", "xlink:href"));
		links.put("output",
				xmlUtils.evalXPath(doc, "//nsr:output", "xlink:href"));
		links.put("securectx",
				xmlUtils.evalXPath(doc, "//nsr:securityContext", "xlink:href"));
		links.put("listeners",
				xmlUtils.evalXPath(doc, "//nsr:listeners", "xlink:href"));

		// get the inputs
		String inputs = server.getRunAttribute(this, links.get("inputs"));
		doc = ParseUtil.parse(inputs);
		links.put("baclava",
				xmlUtils.evalXPath(doc, "//nsr:baclava", "xlink:href"));

		// set io properties
		links.put("io", links.get("listeners") + "/io");
		links.put("stdout", links.get("io") + "/properties/stdout");
		links.put("stderr", links.get("io") + "/properties/stderr");
		links.put("exitcode", links.get("io") + "/properties/exitcode");

		return links;
	}

	private void ls_ports(String dir, List<String> lists, List<String> values,
			boolean top) {

		if (dir.endsWith("/")) {
			dir = dir.substring(0, dir.length() - 1);
		}

		String dirList = server.getRunAttribute(this, links.get("wdir") + "/"
				+ dir, "application/xml");
		Document doc = ParseUtil.parse(dirList);

		if (lists != null) {
			List<Element> dirs = xmlUtils.evalXPath(doc, "//nss:dir");

			// need to make space in the list for random access
			if (dirs.size() > lists.size()) {
				for (int i = 0; i < dirs.size(); i++) {
					lists.add("");
				}
			}

			for (Element e : dirs) {
				String[] paths = e.getTextContent().split("/");
				String name = paths[paths.length - 1];
				if (top) {
					lists.set(0, name);
				} else {
					int index = (Integer.parseInt(xmlUtils.getServerAttribute(
							e, "name")) - 1);
					lists.set(index, name);
				}
			}
		}

		if (values != null) {
			List<Element> files = xmlUtils.evalXPath(doc, "//nss:file");

			// need to make space in the list for random access
			if (files.size() > values.size()) {
				for (int i = 0; i < files.size(); i++) {
					values.add("");
				}
			}

			for (Element e : files) {
				String[] paths = e.getTextContent().split("/");
				String name = paths[paths.length - 1];
				if (top) {
					values.set(0, name);
				} else {
					int index = (Integer.parseInt(xmlUtils.getServerAttribute(
							e, "name")) - 1);
					values.set(index, name);
				}
			}
		}
	}

	private void ls_ports(String dir, List<String> lists, List<String> values) {
		ls_ports(dir, lists, values, true);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private RunOutput<?> getOutput(String port, boolean refs, boolean top) {

		// if we are at the top level we need to return a singleton value here.
		if (top) {
			ArrayList<String> values = new ArrayList<String>();
			ls_ports("out", null, values);

			if (values.contains(port)) {
				if (refs) {
					try {
						return new RunOutputRefs(new URI(links.get("wdir")
								+ "/out/" + port));
					} catch (URISyntaxException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					return new RunOutputData(server.getRunData(this,
							links.get("wdir") + "/out/" + port,
							"application/octet-stream"));
				}
			}
		}

		// this port isn't a singleton so drill into it and build up
		// lists (of lists, etc) of items
		ArrayList<String> lists = new ArrayList<String>();
		ArrayList<String> items = new ArrayList<String>();

		RunOutput result = refs ? new RunOutputRefs() : new RunOutputData();

		ls_ports("out/" + port, lists, items, false);

		// for each list, recurse into it and add the items to the result
		for (String list : lists) {
			result.addNode(getOutput(port + "/" + list, refs, false));
		}

		// each item, add it to the output list
		for (String item : items) {
			if (refs) {
				try {
					result.addNode(new RunOutputRefs(new URI(links.get("wdir")
							+ "/out/" + port + "/" + item)));
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				result.addNode(new RunOutputData(server.getRunData(this,
						links.get("wdir") + "/out/" + port + "/" + item,
						"application/octet-stream")));
			}
		}

		return result;
	}

	public RunOutput<?> getOutput(String port, boolean refs) {
		return getOutput(port, refs, true);
	}

	public RunOutput<?> getOutput(String port) {
		return getOutput(port, false, true);
	}

	public RunOutput<?> getOutputRefs(String port) {
		return getOutput(port, true, true);
	}
}

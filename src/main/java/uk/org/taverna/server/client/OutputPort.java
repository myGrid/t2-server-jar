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

import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import net.sf.practicalxml.util.NodeListIterable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import uk.org.taverna.server.client.connection.URIUtils;

/**
 * 
 * @author Robert Haines
 */
public final class OutputPort extends Port {

	private final PortValue value;

	/**
	 * 
	 * @param run
	 * @param xml
	 */
	OutputPort(Run run, Element xml) {
		super(run, xml);

		value = parse((Element) xml.getFirstChild());
	}

	/**
	 * 
	 * @return
	 */
	public PortValue getValue() {
		return value;
	}

	/**
	 * Does this port contain error?
	 * 
	 * @return <code>true</code> if this port contains an error,
	 *         <code>false</code> otherwise.
	 */
	public boolean isError() {
		return value.isError();
	}

	/**
	 * Get the total size of all the data on this OutputPort. For a singleton
	 * port this is simply the size of the single value but for any other depth
	 * port it is the addition of all values in the port.
	 * 
	 * @return The total data size of this OutputPort.
	 */
	public long getDataSize() {
		return value.getDataSize();
	}

	public String getContentType() {
		return value.getContentType();
	}

	public byte[] getData() {
		return value.getData();
	}

	public byte[] getData(int index) {
		return value.getData(index);
	}

	public String getDataAsString() {
		return value.getDataAsString();
	}

	public URI getReference() {
		return value.getReference();
	}

	@Override
	public String toString() {
		return toString(0);
	}

	public String toString(int indent) {
		String spaces = StringUtils.repeat(" ", indent);
		StrBuilder message = new StrBuilder();
		PrintWriter pw = new PrintWriter(message.asWriter());

		pw.format("%s%s", spaces, getName());
		pw.format("%s (depth %s) {\n", spaces, getDepth());
		pw.format("%s%s\n}", spaces, value.toString(indent + 1));

		return message.toString();
	}

	private PortValue parse(Element node) {
		String name = node.getNodeName();

		if (name.equalsIgnoreCase("port:value")) {
			URI ref = URI.create(xmlUtils.getXlinkAttribute(node, "href"));
			String type = xmlUtils.getPortAttribute(node, "contentType");
			int size = Integer.parseInt(xmlUtils.getPortAttribute(node,
					"contentByteLength"));

			return new PortData(this, ref, type, size);
		} else if (name.equalsIgnoreCase("port:list")) {
			// FIXME: This really is faked bad! The XML doc should have the list
			// ref in it re: http://dev.mygrid.org.uk/issues/browse/TAVSERV-260
			URI ref = URIUtils.appendToPath(run.getServer().getURI(),
					"rest/runs/" + run.getIdentifier() + "/wd/" + getName());
			List<PortValue> list = new ArrayList<PortValue>();
			NodeListIterable nodes = new NodeListIterable(node.getChildNodes());
			for (Node n : nodes) {
				list.add(parse((Element) n));
			}

			return new PortList(this, ref, list);
		} else {
			URI ref = URI.create(xmlUtils.getXlinkAttribute(node, "href"));

			return new PortError(this, ref);
		}
	}
}

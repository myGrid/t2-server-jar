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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.practicalxml.xpath.XPathWrapper;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * This class is for internal use only.
 * 
 * It contains methods for caching the repetitive XML/XPath operations used by
 * the Run and Server classes.
 * 
 * @author Robert Haines
 */
final class XmlUtils {
	private static final XmlUtils instance = new XmlUtils();

	private static final String serverNS = "http://ns.taverna.org.uk/2010/xml/server/";
	private static final String restNS = serverNS + "rest/";
	private static final String xlinkNS = "http://www.w3.org/1999/xlink";

	private final Map<String, String> fragments;
	private final Map<String, XPathWrapper> queries;

	private XmlUtils() {
		// set up message fragments
		fragments = new HashMap<String, String>();
		fragments.put("workflow", "<t2s:workflow xmlns:t2s=\"" + serverNS
				+ "\">\n  %s\n</t2s:workflow>");
		fragments.put("input", "<t2sr:runInput xmlns:t2sr=\"" + restNS
				+ "\">\n  %s\n</t2sr:runInput>");
		fragments.put("inputvalue", String.format(fragments.get("input"),
				"<t2sr:value>%s</t2sr:value>"));
		fragments.put("inputfile", String.format(fragments.get("input"),
				"<t2sr:file>%s</t2sr:file>"));
		fragments.put("upload", "<t2sr:upload xmlns:t2sr=\"" + restNS
				+ "\" t2sr:name=\"%s\">\n  %s\n</t2sr:upload>");
		fragments.put("mkdir", "<t2sr:mkdir xmlns:t2sr=\"" + restNS
				+ "\" t2sr:name=\"%s\" />");

		// initialise compiled queries
		queries = new HashMap<String, XPathWrapper>();
	}

	static XmlUtils getInstance() {
		return instance;
	}

	List<Element> evalXPath(Document doc, String expr) {
		return getQuery(expr).evaluate(doc, Element.class);
	}

	String evalXPath(Document doc, String expr, String attr) {
		return getQuery(expr + "/@" + attr).evaluateAsString(doc);
	}

	String buildXMLFragment(String key, String data) {
		return String.format(fragments.get(key), data);
	}

	String buildXMLFragment(String key, String data1, String data2) {
		return String.format(fragments.get(key), data1, data2);
	}

	String getServerAttribute(Element e, String attr) {
		return e.getAttributeNS(serverNS, attr);
	}

	String getRestAttribute(Element e, String attr) {
		return e.getAttributeNS(restNS, attr);
	}

	private XPathWrapper getQuery(String expr) {
		XPathWrapper query = queries.get(expr);

		if (query == null) {
			query = new XPathWrapper(expr);
			query.bindNamespace("nss", serverNS);
			query.bindNamespace("nsr", restNS);
			query.bindNamespace("xlink", xlinkNS);

			queries.put(expr, query);
		}

		return query;
	}
}

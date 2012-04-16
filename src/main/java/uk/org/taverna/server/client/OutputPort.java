package uk.org.taverna.server.client;

import java.net.URI;
import java.util.List;

import net.sf.practicalxml.util.NodeListIterable;

import org.w3c.dom.Element;
import org.w3c.dom.Node;

import uk.org.taverna.server.client.util.TreeList;

public final class OutputPort extends Port {

	private final TreeList<PortValue> structure;
	private int totalDataSize;
	private boolean error;

	/**
	 * 
	 * @param run
	 * @param xml
	 */
	public OutputPort(Run run, Element xml) {
		super(run, xml);

		totalDataSize = 0;
		error = false;
		structure = parse((Element) xml.getFirstChild());
	}

	/**
	 * 
	 * @param coords
	 * @return
	 * @throws IndexOutOfBoundsException
	 */
	public PortValue getValue(int... coords) throws IndexOutOfBoundsException {
		if (coords.length != depth) {
			throw new IndexOutOfBoundsException("OutputPort of depth " + depth
					+ " accessed as depth " + coords.length + ".");
		}

		return structure.getValue(coords);
	}

	/**
	 * 
	 * @param coords
	 * @return
	 * @throws IndexOutOfBoundsException
	 */
	public PortValue getValue(List<Integer> coords)
			throws IndexOutOfBoundsException {
		if (coords == null || coords.size() == 0) {
			return getValue();
		} else {
			int[] array = new int[coords.size()];
			for (int i = 0; i < array.length; i++) {
				array[i] = coords.get(i);
			}
			return getValue(array);
		}
	}

	/**
	 * Does this port contain error? should this be hasError?
	 * 
	 * @return <code>true</code> if this port contains an error,
	 *         <code>false</code> otherwise.
	 */
	public boolean isError() {
		return error;
	}

	/**
	 * Get the total size of all the data on this OutputPort. For a singleton
	 * port this is simply the size of the single value but for any other depth
	 * port it is the addition of all values in the port.
	 * 
	 * @return The total data size of this OutputPort.
	 */
	public int getTotalDataSize() {
		return totalDataSize;
	}

	@Override
	public String toString() {
		return structure.toString();
	}

	/*
	 * Side-effect warning: This method sets this.error and this.totalDataSize!
	 */
	private TreeList<PortValue> parse(Element node) {
		String name = node.getNodeName();

		if (name.equalsIgnoreCase("port:value")) {
			URI ref = URI.create(xmlUtils.getXlinkAttribute(node, "href"));
			String type = xmlUtils.getPortAttribute(node, "contentType");
			int size = Integer.parseInt(xmlUtils.getPortAttribute(node,
					"contentByteLength"));
			totalDataSize += size;
			PortValue value = new PortValue(this, ref, type, size);
			return new TreeList<PortValue>(value);
		} else if (name.equalsIgnoreCase("port:list")) {
			TreeList<PortValue> list = new TreeList<PortValue>();
			NodeListIterable nodes = new NodeListIterable(node.getChildNodes());
			for (Node n : nodes) {
				list.addNode(parse((Element) n));
			}
			return list;
		} else {
			this.error = true;
			URI error = URI.create(xmlUtils.getXlinkAttribute(node, "href"));
			PortValue value = new PortValue(this, error);
			return new TreeList<PortValue>(value);
		}
	}
}

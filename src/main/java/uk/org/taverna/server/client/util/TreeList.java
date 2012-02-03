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

package uk.org.taverna.server.client.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * The TreeList class represents an arbitrary depth list structure where each
 * node, including the root, can be either a single value, or a list of child
 * nodes.
 * 
 * The list of child node, should it exist, acts like a {@link java.util.List}.
 * 
 * @author Robert Haines
 */
public class TreeList<E> implements Cloneable, Iterable<TreeList<E>> {

	// the children of this node
	protected List<TreeList<E>> branch;

	// the payload of this node
	protected E leaf;

	// can this node be both a leaf and a branch?
	private final boolean composite;

	/**
	 * Create a new branch node with no initial value.
	 */
	public TreeList() {
		this(false);
	}

	/**
	 * Create a new node with no initial value.
	 * 
	 * @param composite
	 *            true for this node to be composite, false otherwise.
	 */
	public TreeList(boolean composite) {
		this.composite = composite;
		leaf = null;
		branch = new ArrayList<TreeList<E>>();
	}

	/**
	 * Create a new leaf node with an initial value.
	 * 
	 * @param value
	 *            the initial value.
	 */
	public TreeList(E value) {
		this(false, value);
	}

	/**
	 * Create a new node with an initial value.
	 * 
	 * @param composite
	 *            true for this node to be composite, false otherwise.
	 * @param value
	 *            the initial value.
	 */
	public TreeList(boolean composite, E value) {
		this.composite = composite;
		leaf = value;
		branch = composite ? new ArrayList<TreeList<E>>() : null;
	}

	/**
	 * Create a new node with an initial branch of child nodes.
	 * 
	 * @param composite
	 *            true for this node to be composite, false otherwise.
	 * @param list
	 *            the initial branch of child nodes.
	 */
	protected TreeList(boolean composite, List<TreeList<E>> list) {
		this.composite = composite;
		leaf = null;
		branch = list != null ? list : new ArrayList<TreeList<E>>();
	}

	/**
	 * Create a new node with an initial branch of child nodes.
	 * 
	 * @param list
	 *            the initial branch of child nodes.
	 */
	protected TreeList(List<TreeList<E>> list) {
		this(false, list);
	}

	/**
	 * Create a new node with an initial value and branch of child nodes.
	 * 
	 * @param value
	 *            the initial value.
	 * @param list
	 *            the initial branch of child nodes.
	 */
	protected TreeList(E value, List<TreeList<E>> list) {
		composite = true;
		leaf = value;
		branch = list != null ? list : new ArrayList<TreeList<E>>();
	}

	/**
	 * Is this node a leaf? Composite nodes are both leaf and branch nodes.
	 * 
	 * @return true if this node is a leaf, false otherwise.
	 */
	public boolean isLeaf() {
		return composite || branch == null;
	}

	/**
	 * Is this node a branch? Composite nodes are both leaf and branch nodes.
	 * 
	 * @return true if this node is a branch, false otherwise.
	 */
	public boolean isBranch() {
		return composite || branch != null;
	}

	/**
	 * Is this node a composite node?
	 * 
	 * @return true if this is a composite node, false otherwise.
	 */
	public boolean isComposite() {
		return composite;
	}

	/**
	 * Get the value stored in this node.
	 * 
	 * @return the value stored in this node.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a leaf node.
	 */
	public E getValue() throws TreeListTypeMismatchException {
		if (!isLeaf()) {
			throw new TreeListTypeMismatchException("leaf");
		}

		return leaf;
	}

	public E getValue(int... coords) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		int coord = coords[0];

		if (coords.length == 1) {
			return getNode(coord).getValue();
		} else {
			int[] newCoords = Arrays.copyOfRange(coords, 1, coords.length);
			return getNode(coord).getValue(newCoords);
		}
	}

	/**
	 * Set the value stored in this node.
	 * 
	 * @param data
	 *            the new value of this node.
	 * @return the old value of this node.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a leaf node.
	 */
	public E setValue(E data) throws TreeListTypeMismatchException {
		if (!isLeaf()) {
			throw new TreeListTypeMismatchException("leaf");
		}

		E temp = leaf;
		leaf = data;
		return temp;
	}

	/**
	 * Get a node from the child branch of this node.
	 * 
	 * @param index
	 *            the index of the node in the child branch to return.
	 * @return the node from the child branch of this node.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public TreeList<E> getNode(int index) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.get(index);
	}

	/**
	 * Set a node in the child branch of this node.
	 * 
	 * @param index
	 *            the index of the node in the child branch to change.
	 * @param node
	 *            the new node.
	 * @return the old node.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public TreeList<E> setNode(int index, TreeList<E> node)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		TreeList<E> temp = branch.get(index);
		branch.set(index, node);
		return temp;
	}

	/**
	 * Add a node to the end of this node's child branch.
	 * 
	 * @param node
	 *            the node to add.
	 * @return true
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public boolean addNode(TreeList<E> node)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.add(node);
	}

	/**
	 * Insert a node at the specified position in the child branch. Shifts the
	 * node currently at that position (if any) and any subsequent nodes to the
	 * right (adds one to their indices).
	 * 
	 * @param index
	 *            index at which the specified node is to be inserted.
	 * @param node
	 *            node to be inserted.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public void addNode(int index, TreeList<E> node)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		branch.add(index, node);
	}

	/**
	 * Removes the node at the specified position in this list. Shifts any
	 * subsequent nodes to the left (subtracts one from their indices). Returns
	 * the node that was removed from the list.
	 * 
	 * @param index
	 *            the index of the node to be removed.
	 * @return the node previously at the specified position.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public TreeList<E> removeNode(int index)
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.remove(index);
	}

	/**
	 * Removes the first occurrence of the specified node from this list, if it
	 * is present (optional operation). If this list does not contain the node,
	 * it is unchanged.
	 * 
	 * @param o
	 *            node to be removed from this list, if present.
	 * @return true if this list contained the specified element.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public boolean removeNode(Object o) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.remove(o);
	}

	/**
	 * Returns true if this list contains the specified node. More formally,
	 * returns true if and only if the child branch of this node contains at
	 * least one node e such that (o==null ? e==null : o.equals(e)).
	 * 
	 * @param o
	 *            node whose presence in this list is to be tested.
	 * @return true if this list contains the specified element.
	 * @throws TreeListTypeMismatchException
	 *             if this is not a branch node.
	 */
	public boolean containsNode(Object o) throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.contains(o);
	}

	/**
	 * Removes all of the nodes from this node's branch and sets the value of
	 * this node to null.
	 */
	public void clear() {
		leaf = null;

		if (isBranch()) {
			branch.clear();
		}
	}

	/**
	 * Returns the number of nodes on this branch or 0 if it is a leaf nodes.
	 * 
	 * @return the number of nodes on this branch or 0 if it is a leaf nodes.
	 */
	public int size() {
		return isBranch() ? branch.size() : 0;
	}

	/**
	 * Returns a string representation of this TreeList. If this node is a leaf
	 * it simply returns a representation of its value. If it is a branch then
	 * it recursively calls toString() on each child.
	 * 
	 * @return a string representation of this TreeList.
	 */
	@Override
	public String toString() {
		String result = "";

		if (isLeaf()) {
			result = leaf == null ? "null" : leaf.toString();
		}

		if (isComposite()) {
			result += ": ";
		}

		if (isBranch()) {
			if (branch.size() == 0) {
				result += "[null]";
			} else {
				result += "[" + branch.get(0);
				for (int i = 1; i < branch.size(); i++) {
					result += ", " + branch.get(i);
				}
				result += "]";
			}
		}

		return result;
	}

	/**
	 * Returns a shallow copy of this TreeList instance. (The value and node
	 * elements themselves are not copied.)
	 * 
	 * @return a clone of this TreeList instance
	 */
	@Override
	public Object clone() {
		if (this.isComposite()) {
			return new TreeList<E>(this.leaf, this.branch);
		} else {
			if (this.isLeaf()) {
				return new TreeList<E>(this.leaf);
			} else {
				return new TreeList<E>(this.branch);
			}
		}
	}

	/**
	 * Returns an iterator over the child nodes of this node, if it has any.
	 * 
	 * @return an iterator.
	 */
	@Override
	public Iterator<TreeList<E>> iterator()
			throws TreeListTypeMismatchException {
		if (!isBranch()) {
			throw new TreeListTypeMismatchException("branch");
		}

		return branch.iterator();
	}
}

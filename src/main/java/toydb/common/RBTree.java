package toydb.common;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import toydb.toydb.Data;

/**
 * An implementation of Red-Black Tree for String key and values.
 *
 * <p>Properties of a red-black tree : - Every node is either red or black - Root node is always
 * black - Leaf nodes (null) are always considered black - If a node is Red, its children have to be
 * black (i.e. two red nodes cannot be present one after another in the same hierarchy) - For each
 * node, all simple paths from the node to the descedant leaves contain the same number of black
 * nodes.
 */
public class RBTree {

  enum Color {
    RED,
    BLACK
  }

  class Node {
    String key;
    String value;

    Color color;

    Node left;
    Node right;
    Node parent;

    Node(String key, String value) {
      this.key = key;
      this.value = value;
      this.left = null;
      this.right = null;
      this.parent = null;
      this.color = Color.RED;
    }

    @Override
    public String toString() {
      String leftKey = left == null ? null : left.key;
      String rightKey = right == null ? null : right.key;
      String parentKey = parent == null ? null : parent.key;
      return key
          + "\t:"
          + value
          + "\t\t:"
          + color
          + "\t:Left("
          + leftKey
          + ")\t:Right("
          + rightKey
          + ")\t:Parent("
          + parentKey
          + ")";
    }
  }

  class NodeWrapper {
    Node node;

    NodeWrapper() {
      node = null;
    }
  }

  private Node root;

  private int sizeInBytes = 0;

  public void insert(String key, String value) {
    insert(new Node(key, value));
    sizeInBytes += key.length();
    sizeInBytes += value.length();
  }

  public String get(String key) {
    return find(key);
  }

  private String find(String key) {
    Node curr = root;
    while (curr != null) {
      if (key.equals(curr.key)) return curr.value;
      else if (key.compareTo(curr.key) < 0) curr = curr.left;
      else curr = curr.right;
    }

    return null;
  }

  public Pair<Data, Data> getInorderSuccessorPredecessor(String key) {
    NodeWrapper predecessorNode = new NodeWrapper();
    NodeWrapper successorNode = new NodeWrapper();
    getInorderSuccessorPredecessor(key, root, predecessorNode, successorNode);

    Data predecessor =
        predecessorNode.node != null
            ? new Data(predecessorNode.node.key, predecessorNode.node.value)
            : null;
    Data successor =
        successorNode.node != null
            ? new Data(successorNode.node.key, successorNode.node.value)
            : null;

    return new Pair<>(predecessor, successor);
  }

  private void getInorderSuccessorPredecessor(
      String key, Node root, NodeWrapper predecessor, NodeWrapper successor) {
    if (root == null) return;

    getInorderSuccessorPredecessor(key, root.left, predecessor, successor);

    if (root.key.compareTo(key) < 0) {
      predecessor.node = root;
    } else {
      successor.node = root;
    }

    getInorderSuccessorPredecessor(key, root.right, predecessor, successor);
  }

  public List<Data> getAllElements() {
    List<Node> inorderNodes = new ArrayList<>();
    inorder(inorderNodes, root);

    return inorderNodes.stream().map(n -> new Data(n.key, n.value)).collect(Collectors.toList());
  }

  public List<Node> getAllNodes() {
    List<Node> inorderNodes = new ArrayList<>();
    inorder(inorderNodes, root);

    return inorderNodes;
  }

  private void inorder(List<Node> list, Node root) {
    if (root == null) return;
    if (root.left == null && root.right == null) list.add(root);
    else {
      inorder(list, root.left);
      list.add(root);
      inorder(list, root.right);
    }
  }

  /**
   * Insert a node as RED following BST properties, and then recolor the tree, since the red-black
   * property might be violated now.
   *
   * @param node
   */
  private void insert(Node node) {
    Node curr = root;
    Node prev = null;

    while (curr != null) {
      prev = curr;
      curr = (node.key.compareTo(curr.key) <= 0) ? curr.left : curr.right;
    }

    if (prev == null) {
      root = node;
    } else if (node.key.compareTo(prev.key) <= 0) {
      prev.left = node;
      node.parent = prev;
    } else {
      prev.right = node;
      node.parent = prev;
    }

    insertFixUp(node);
  }

  /**
   * The possible violations by inserting a red node can be categorized by the following cases : -
   * CASE 1 : parent and uncle of the inserted node are both red - CASE 2 : parent red, uncle black,
   * and inserted node is right child of parent - CASE 3 : parent red, uncle black, and inserted
   * node is left child of parent
   *
   * @param node
   */
  private void insertFixUp(Node node) {
    while (node.parent != null && node.parent.color.equals(Color.RED)) {

      // STEP 0 : parent is left child and uncle is right child of grandparent
      if (node.parent.parent.left == node.parent) {

        Node u = node.parent.parent.right;

        // CASE 1 : u is RED which means u is not null [null is considered as black]
        if (u != null && u.color.equals(Color.RED)) {
          node.parent.color = Color.BLACK;
          u.color = Color.BLACK;
          node.parent.parent.color = Color.RED;
          node = node.parent.parent;
        }
        // CASE 2:
        else if ((u == null || u.color.equals(Color.BLACK)) && (node == node.parent.right)) {
          node = node.parent;
          leftRotate(node);
        }
        // CASE 3: u is BLACK or null, and node is left child of its parent
        else {
          node.parent.color = Color.BLACK;
          node.parent.parent.color = Color.RED;
          rightRotate(node.parent.parent);
        }
      } else { // parent is right child, and uncle is left child of parent

        Node u = node.parent.parent.left;

        // CASE 1 : u is RED which means u is not null [null is considered as black]
        if (u != null && u.color.equals(Color.RED)) {
          node.parent.color = Color.BLACK;
          u.color = Color.BLACK;
          node.parent.parent.color = Color.RED;
          node = node.parent.parent;
        }
        // CASE 2:
        else if ((u == null || u.color.equals(Color.BLACK)) && (node == node.parent.left)) {
          node = node.parent;
          rightRotate(node);
        }
        // CASE 3: u is BLACK or null, and node is right child of its parent
        else {
          node.parent.color = Color.BLACK;
          node.parent.parent.color = Color.RED;
          leftRotate(node.parent.parent);
        }
      }
    }
    root.color = Color.BLACK;
  }

  /**
   * leftRotates the subtree rooted at X. It assumes that the right subtree Y of X is not null. STEP
   * 01 : Say parent of X is P. If X is left of P, set left of P to Y, else right of P to Y STEP 02
   * : Set Right of X = Left of Y STEP 03 : Set Left of Y = X
   *
   * @param x : The subtree rooted at x, which needs to be left rotated
   */
  private void leftRotate(Node x) {
    Node p = x.parent;
    Node y = x.right;

    // STEP 01
    if (p == null) {
      root = y;
      y.parent = p;
    } else if (p.left == x) {
      p.left = y;
      y.parent = p;
    } else {
      p.right = y;
      y.parent = p;
    }

    // STEP 02
    x.right = y.left;
    if (x.right != null) x.right.parent = x;

    // STEP 03
    y.left = x;
    x.parent = y;
  }

  /**
   * rightRotates the subtree rooted at Y. It assumes the left subtree X of Y is not null. STEP 01 :
   * change parent of x = parent of y STEP 02 : y.left = x.right STEP 03 : x.right = y
   *
   * @param y
   */
  private void rightRotate(Node y) {
    Node p = y.parent;
    Node x = y.left;

    // STEP 01
    if (p == null) {
      root = x;
      root.parent = null;
    } else if (p.left == y) {
      p.left = x;
      x.parent = p;
    } else {
      p.right = x;
      x.parent = p;
    }

    // STEP 02:
    y.left = x.right;
    if (y.left != null) y.left.parent = y;

    // STEP 03:
    x.right = y;
    y.parent = x;
  }

  public int getSizeInBytes() {
    return sizeInBytes;
  }
}

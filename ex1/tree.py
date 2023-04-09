from __future__ import annotations
from typing import List, Dict
from collections import deque

from ex1.utils import Bucket


class Node:
    "this class represents a None in a tree"

    def __init__(self, key: int, data=None):
        """
        @param key: int, key identifier of the node
        @param data: [None, int, Bucket], data inside node
        """
        self.key: int = key
        self.data: [None, int, Bucket] = data
        self.left: [None, Node] = None
        self.right: [None, Node] = None
        self.parent: [None, Node] = None

    def __str__(self):
        return str(self.key)

    def __repr__(self):
        return f'Node-{self.key}'


class BinaryTree:
    "this class represents a Binary Tree instance"

    def __init__(self, root: Node):
        """
        @param root: Node, root node of the binary tree
        """
        self.root: Node = root
        self.levels: List[List[Node]] = []
        self.leaves: List[Node] = []
        self.nodes: List[Node] = []
        self.leaf_to_path: Dict[int, List[int]] = dict()
        self.node_to_leaves: Dict[int, List[int]] = dict()
        self.height: int = self._set_height(root, -1)
        self._set_nodes()
        self._set_levels()
        self._init_root_leaf_paths()
        self._assign_node_to_reachable_leaves()

        self.leaf_keys = {leaf.key for leaf in self.leaves}
        self.node_keys = {node.key for node in self.nodes}

        assert self.root.parent is None

    def __str__(self):
        return f'Root-{self.root.key}'

    def __repr__(self):
        return f'BTree-{len(self.nodes)}'

    # =============================== PRIVATE_METHODS =============================== #
    def _set_height(self, node: Node, height: int) -> int:
        """
        computes the height of the tree
        @param node: Node, node to calculate the height from
        @param height: int, height of tree up to this node
        @return: max_height: int, maximal height from given node
        """
        if node is None:
            return height
        h_left = self._set_height(node.left, height + 1)
        h_right = self._set_height(node.right, height + 1)
        return max([h_left, h_right])

    def _set_nodes(self) -> None:
        "creates sets of nodes and leaves in the tree"
        visited = set()
        leaves = set()
        queue = deque([self.root])
        while len(queue) > 0:
            node = queue.popleft()
            if node not in visited:
                visited.add(node)
                if node.left is not None:
                    queue.insert(-1, node.left)
                if node.right is not None:
                    queue.insert(-1, node.right)
                if node.left is None and node.right is None:
                    leaves.add(node)
        self.nodes = list(visited)
        self.leaves = list(leaves)

    def _set_levels(self) -> None:
        "create list of levels in the binary tree"
        # root is at level 0
        levels = [[self.root]]
        self.nodes.sort(key=lambda node: node.key)
        for i in range(1, self.height + 1):
            # compute all nodes in level i
            levels.append(self.nodes[(2 ** i) - 1:2 ** (i + 1) - 1])
        self.levels = levels

    def _init_root_leaf_paths(self) -> None:
        "calculates all paths from root node to leaf nodes"
        # for every leaf, compute its unique path from the root node
        for leaf in self.leaves:
            # map every leaf key to the path from the root (by node keys)
            self.leaf_to_path[leaf.key] = [leaf.key]
            parent = leaf.parent
            # continue until reaching the root node
            while parent is not None:
                self.leaf_to_path[leaf.key].insert(0, parent.key)
                parent = parent.parent  # update parent

    def _assign_node_to_reachable_leaves(self) -> None:
        "maps every node in the tree to reachable leaves in the tree"
        # assign every node in the tree to list of reachable leaves
        for node in self.nodes:
            self.node_to_leaves[node.key] = []
            for leaf in self.leaves:
                if node.key in self.get_root_path(leaf.key):
                    self.node_to_leaves[node.key].append(leaf.key)

    # =============================== API_METHODS =============================== #

    @staticmethod
    def build(node_keys: List[int]) -> BinaryTree:
        """
        builds a binary tree instance from list of node keys
        @param node_keys: list[int], list of node keys to generate binary tree from
        @return: tree: BinaryTree, binary tree instance, key in list[0] will be the root node
        """
        assert isinstance(node_keys, list)
        nodes = []
        for key in node_keys:
            nodes.append(Node(key))
        # if node key is i:
        # left child is at 2i+1
        # right child is at 2i+2
        # and parent is at (i-1)//2
        for node in nodes:
            try:
                node.parent = nodes[(node.key - 1) // 2] if node.key > 0 else None
                node.left = nodes[(node.key * 2) + 1]
                node.right = nodes[(node.key * 2) + 2]
            except IndexError:
                continue
        return BinaryTree(nodes[0])

    def get_root_path(self, key: int) -> List[int]:
        """
        @param key: int, key of node in the tree
        @return: path: [list[int], None], path from root node to leaf node if this path exists, None otherwise
        """
        return self.leaf_to_path.get(key)

    def get_reachable_leaves(self, key: int) -> [None, List[int]]:
        """
        @param key: int, key of node in the tree
        @return: [list[int], None], list of keys of reachable leaves in the tree, None otherwise
        """
        return self.node_to_leaves.get(key)


def bucket_bfs(tree: BinaryTree, target_values: List[str]):
    """
    search for nodes in the tree that hold given values
    @param tree: BinaryTree, tree to search in for values
    @param target_values: list[str], values to search for in the tree
    @return: target_target_nodes: Set[Node], set of nodes that given values are located in
    """
    found = []
    empty = 0
    visited = set()
    target_nodes = set()
    queue = deque([tree.root])
    while len(queue) > 0:
        node = queue.popleft()
        if node not in visited and node is not None:
            visited.add(node)
            queue.insert(-1, node.left)
            queue.insert(-1, node.right)
            for block in node.data.get_array():
                idx = block.data.find(';')
                name, data = block.data[:idx], block.data[idx + 1:]
                if name in target_values:
                    found.append(name)
                    target_nodes.add(node)
                else:
                    empty += 1
    # return set of nodes that contain the target data
    print(f'(bucket_bfs)\nfound {len(found)} files: {found}\nempty blocks: {empty}\ntotal: {len(found) + empty}')
    return target_nodes

from queue import Queue

# BFS implementation

class Node:
    def __init__(self, data):
        self.data = data
        self.parent = None
        self.children = dict()

    def add_child(self, child):

        child.parent = self
        self.children[child.data] = child


class Tree:

    def __init__(self, root):

        self.root = root

    def get_position(self, data, depth=None):

        visited, queue = set(), Queue()
        queue.put((self.root, 0))

        while not queue.empty():

            node, level = queue.get()

            if depth is not None and level > depth:
                break

            if depth is None:
                if node.data == data:
                    return node
            else:
                if level == depth and node.data == data:
                    return node

            for leaf in node.children.values():
                if leaf in visited:
                    continue
                queue.put((leaf, level + 1))

            visited.add(node)

        return None

    def add_to_tree(self, leaf, parent):

        node = self.get_position(parent.data)
        if node is not None:
            node.add_child(leaf)
            return True
        else:
            return False

    def insert(self, leaf, parent):
        return self.add_to_tree(leaf, parent)

    def parent(self, data):
        node = self.get_position(data)
        return node.parent if not None else None

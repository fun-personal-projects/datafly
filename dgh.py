import csv
from io import StringIO
from tree import Node, Tree


class _DGH:

    def __init__(self, dgh_path):

        self.hierarchies = dict()
        self.gen_levels = dict()

    def generalize(self, value, gen_level=None):

        for hierarchy in self.hierarchies:
            if gen_level is None:
                node = self.hierarchies[hierarchy].get_position(value)
            else:
                node = self.hierarchies[hierarchy].get_position(
                    value,
                    self.gen_levels[hierarchy] - gen_level)     # Depth.

            if node is None:
                continue
            elif node.parent is None:
                return None
            else:
                return node.parent.data
        raise KeyError(value)


class CsvDGH(_DGH):

    def __init__(self, dgh_path):

        super().__init__(dgh_path)

        try:
            with open(dgh_path, 'r') as file:
                for line in file:

                    try:
                        csv_reader = csv.reader(StringIO(line))
                    except IOError:
                        raise
                    values = next(csv_reader)

                    if values[-1] not in self.hierarchies:
                        self.hierarchies[values[-1]] = Tree(Node(values[-1]))
                        self.gen_levels[values[-1]] = len(values) - 1
                    self._insert_hierarchy(
                        values[:-1], self.hierarchies[values[-1]])

        except FileNotFoundError:
            raise
        except IOError:
            raise

    @staticmethod
    def _insert_hierarchy(values, tree):
        current_node = tree.root

        for i, value in enumerate(reversed(values)):

            if value in current_node.children:
                current_node = current_node.children[value]
                continue
            else:
                for v in list(reversed(values))[i:]:
                    current_node.add_child(Node(v))
                    current_node = current_node.children[v]
                return True

        return False

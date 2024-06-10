class Node:
    def __init__(self, key, borders: dict):
        self.key = key
        self.borders = borders
        self.left = None
        self.right = None

class BTS:
    def __init__(self):
        self.root = None

    def find(self, node, value):
        current = node
        while current is not None:
            if value == current.key:
                return current, True
            elif value < current.key:
                if current.left is None:
                    return current, False
                current = current.left
            else:
                if current.right is None:
                    return current, False
                current = current.right
        return None, False

    def append(self, obj):
        if self.root is None:
            self.root = obj
            return
        node, fl_find = self.find(self.root, obj.key)
        if fl_find:
            node.borders +=(obj.borders)
        elif not fl_find and node:
            if obj.key < node.key:
                node.left = obj
            else:
                node.right = obj

    def search(self, value):
        current = self.root
        while current is not None:
            if value == current.key:
                return current.borders
            elif value < current.key:
                current = current.left
            else:
                current = current.right
        return None

    def show(self):
        if self.root is None:
            return
        result = []
        stack = []
        current = self.root
        while stack or current:
            while current is not None:
                stack.append(current)
                current = current.left
            current = stack.pop()
            result.append((current.key, current.borders))
            current = current.right
        for key, borders in result:
            print(key, borders)


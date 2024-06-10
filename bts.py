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
        if value == node.key:
            return node, True
        if value < node.key:
            if node.left:
                return self.find(node.left, value)
        elif value > node.key:
            if node.right:
                return self.find(node.right, value)
        return node, False

    def append(self, obj):
        if self.root == None:
            self.root = obj
            return None
        node, fl_find = self.find(self.root, obj.key)
        if fl_find:
            node.borders += obj.borders
            return
        if not fl_find and node:
            if obj.key < node.key:
                node.left = obj
            else:
                node.right = obj

    def search(self, node, value):
        if node.key == value:
            return node.borders
        elif value < node.key:
            return self.search(node.left, value)
        elif value > node.key:
            return self.search(node.right, value)

    def show(self, node):
        if node == None:
            return
        self.show(node.left)
        print(node.key, node.borders)
        self.show(node.right)
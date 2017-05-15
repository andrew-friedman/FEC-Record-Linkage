


class BinaryTree:
	'''
	An object that can keep track of and quickly check whether 
	and an object has been previously added.
	'''

	def __init__(self):
		self.val = None
		self.g = None # g is always greater than val
		self.l = None # l is always less than val

	def add(self, val):
		'''
		Adds val to tree if not already added
		'''
		val = hash(val)
		if self.val is None:
			self.val = val
		else:
			if val > self.val:
				if self.g is None:
					self.g = BinaryTree()
				self.g.add(val)
			elif val < self.val:
				if self.l is None:
					self.l = BinaryTree()
				self.l.add(val)

	def contains(self, val):
		'''
		Return True if val is in tree, False otherwise
		'''
		val = hash(val)
		if self.val is not None:
			if val == self.val:
				return True
			elif val > self.val:
				if self.g is not None:
					return self.g.contains(val)
				else:
					return False
			elif val < self.val:
				if self.l is not None:
					return self.l.contains(val)
				else:
					return False
		else:
			return False

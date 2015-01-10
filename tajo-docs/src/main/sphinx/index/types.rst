*************************************
Index Types
*************************************

Currently, Tajo supports only one type of index, ``TWO_LEVEL_BIN_TREE``, shortly ``BST``. The BST index is a kind of binary search tree which is extended to be permanently stored on disk. It consists of two levels of nodes; a leaf node indexes the keys with the positions of data in an HDFS block and a root node indexes the keys with the leaf node indices.

When an index scan is started, the query engine first reads the root node and finds the search key. If it finds a leaf node corresponding to the search key, it subsequently finds the search key in that leaf node. Finally, it directly reads a tuple corresponding to the search key from HDFS.
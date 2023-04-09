import math
from ex1.utils import *
from ex1.tree import *

SERVER = 'SERVER:'


class Server:
    """
    this class represents a server instance that implements oblivious RAM technique using perfect binary tree
    """

    def __init__(self, num_leaves: int):
        """
        @param num_leaves: int, number of leaves in the oram tree.
        """
        if not isinstance(num_leaves, int) or num_leaves <= 0:
            raise ValueError(f'{SERVER} number of leaves in server must be a positive integer')
        # set the number of leaves to be an exponent of 2
        expo = math.ceil(math.log2(num_leaves))
        # amount of data blocks
        self.num_leaf: int = 2 ** expo
        # total number of nodes in binary tree
        self.num_nodes: int = 2 ** (expo + 1) - 1
        # ORAM binary tree instance
        self.oram: BinaryTree = BinaryTree.build(list(range(self.num_nodes)))
        # each bucket holds log(n) files
        self.bucket_size: int = expo + 1
        # mapping from each node key to the node instance
        self.key_to_node: Dict[int, Node] = {node.key: node for node in self.oram.nodes}
        # initialize node bucket
        for i, node in enumerate(self.oram.nodes):
            node.data = Bucket(self.bucket_size, node.key, leaf_keys=self.oram.get_reachable_leaves(node.key))
        # set key pair for the server
        pr_key, pb_key = generate_key_pair('pr_key_server', 'pb_key_server')
        self.private_key: rsa.RSAPrivateKey = pr_key
        self.public_key: rsa.RSAPublicKey = pb_key

    # =============================== API_METHODS =============================== #

    def owrite(self, file: str, leaf_id: int) -> None:
        """
        ORAM write operation. wirtes given file, which assigned to given leaf_id, into the root node
        @param file: str, file to write into the oram tree
        @param leaf_id: int, leaf key which given file is assigned to
        @return:
        """
        # write new block to root node
        self.oram.root.data.write_data(file, leaf_id)

    def oread(self, key: int) -> Bucket:
        """
        ORAM read operation.
        @param key: int, key of node inside the tree
        @return: bucket: Bucket, bucket inside the corresponding node
        """
        node = self.key_to_node.get(key, None)
        if node is not None:
            return node.data
        raise ValueError(f'{SERVER} node with key ({key}) does not exist in the tree')

    def get_public_key(self) -> rsa.RSAPublicKey:
        """
        @return: pb_key: PublicKey, public key of server
        """
        return self.public_key

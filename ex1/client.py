from typing import List, Dict, Set, Tuple
import sys

from ex1.utils import *
from ex1.server import Server
from ex1.tree import Node, bucket_bfs

NO_NAME = 'NO_FILENAME'
NO_DATA = 'NO_DATA'
CLIENT = 'CLIENT:'

# todo: remember to set this to false before measuring
DEBUG = False


class Client:
    """
    class that represents a Client instance
    """

    def __init__(self, server: Server):
        """
        @param server: Server, server to be registered to
        """
        # mapping from file name to a leaf id number
        self.file_to_leaf: Dict[str, int] = dict()
        self.min_leaf: int = min(server.oram.leaf_keys)
        self.max_leaf: int = self.min_leaf + server.num_leaf - 1

        pr_key, pb_key = generate_key_pair('pr_key_client', 'pb_key_client')
        self.private_key: rsa.RSAPrivateKey = pr_key
        self.public_key: rsa.RSAPublicKey = pb_key
        self.known_hosts: Set[rsa.RSAPublicKey] = {server.get_public_key()}
        # mapping from each filename to client's signature over the hash of the data
        # this is for authenticate the data received from the server
        self.file_to_sig: Dict[str, bytes] = dict()

    # =============================== API_METHODS =============================== #

    def write(self, server: Server, filename: str, data: str) -> None:
        """
        * read all buckets in the path
        * write item to the root
        * re-encrypt root
        * upload encrypted version of nodes read
        * assign new item to a leaf node randomly

        @param server: Server, server instance this client is registered to
        @param filename: str, name of file to upload to server
        @param data: str, data inside file named filename
        @return:
        """
        # concatenate filename and data into one stirng
        file = f'{filename};{data}'
        # assign random leaf id for filename
        leaf_id = self._assign_to_leaf(filename)
        # dummy read procedure, thus, all read/write actions look the same
        path = server.oram.get_root_path(leaf_id)
        for node_id in path:
            bucket = server.oread(node_id)  # dummy read
            self._decrypt_bucket(bucket)
            # re-encrypt bucket in node
            self._encrypt_bucket(bucket)
        # maintain ORAM tree: flush blocks downwards to free space in the root node
        self.flush(server)
        # write write encrypted data to root node
        root_bucket = server.oread(0)
        self._decrypt_bucket(root_bucket)
        server.owrite(file, leaf_id)
        self._encrypt_bucket(root_bucket)
        # sign the data for authenticating it later when recovered from the server
        # signature is over bytes of plain file
        self._sign_file(filename, file)

    def read(self, server: Server, filename: str, delete=False) -> [None, str]:
        """
        * read all buckets in the path
        * write item to the root + delete it from original node
        * re-encrypt root
        * upload encrypted version of nodes client was reading
        * assign item to new leaf

        @param server: Server, server instance this client is registered to
        @param filename: str, name of file to read from server
        @param delete: bool, indicator for delete request
        @return: requested_file: data corresponding to given filename, None of file was not found in the server
        """
        # requested file from the server. initially set to None.
        requested_file = None
        # get path from root to target leaf
        path = server.oram.get_root_path(self.file_to_leaf.get(filename))
        if path is None:
            raise ValueError(f'filename \"{filename}\" does not exist on the client side')
        # read all buckets on the path from the root node to the leaf node
        for node_id in path:
            bucket = server.oread(node_id)
            self._decrypt_bucket(bucket)
            for block in bucket.get_array():
                name, data = self._extract_file(block.data)
                file_plain = f'{name};{data}'
                if name == filename:
                    # authenticate file before using it
                    if not verify(self.public_key, file_plain.encode(), self.file_to_sig[filename]):
                        raise ValueError(f'{CLIENT} file found, but authentication failed')
                    requested_file = data
                    "# do something with the data..."
                    # print(f'{CLIENT} name: {name}; data: {data}')
                    # clear block
                    block.clear()
                    # if client wants to delete the file, encrypt empty file
                    if delete:
                        file_plain = EMPTY_DATA
                    else:
                        self._sign_file(filename, file_plain)
                    # assign to a new leaf id, and write item to root node
                    new_leaf_id = self._assign_to_leaf(filename)
                    server.owrite(file_plain, new_leaf_id)
            # re-encrypt the bucket after read operation
            self._encrypt_bucket(bucket)
        # flush buckets: this makes the same oread and owrite calls look the same
        # and also frees ones block in the root for the file we want to read
        self.flush(server)
        return requested_file

    def delete(self, server: Server, filename: str) -> None:
        """
        deletes file with filename from the server
        @param server:
        @param filename: str, name of file to delete from server
        @return:
        """
        if self.read(server, filename, delete=True):
            return
        raise FileNotFoundError(f'{CLIENT} deleting {filename} failed')

    def flush(self, server: Server) -> [None, List[str]]:
        """
        flush request from the client.
        eventually, the root is going to run out put space
        for each level in the ORAM tree:
          1. choose two nodes randomly
          2. choose one file from each node's bucket (also randomly)
          3. push file to lower level in the tree (write it to correct child)
          4. read + re-encrypt both corresponding children

        @param server: Server, server this client is registered to
        @return: None if no client data was deleted from the server. otherwise, List[str] containing the removed data.
        """
        deleted_data = []
        for level in server.oram.levels:
            # choose 2 nodes randomly (replacement is allowed)
            node1, node2 = random.choices(level, k=2)
            # decrypt chosen buckets
            self._decrypt_bucket(node1.data)
            if node1 != node2:
                self._decrypt_bucket(node2.data)
            # choose blocks randomly from corresponding buckets
            block1 = random.choice(node1.data.get_array())
            block2 = random.choice(node2.data.get_array())
            # push blocks to their correct node: node which is a part of the path
            data = self._push_down(block1, node1, server)
            if data is not None:
                deleted_data.append(data)
            # do not push again if the same block was sampled
            if block1 != block2:
                assert block1.bid != block2.bid
                data = self._push_down(block2, node2, server)
                if data is not None:
                    deleted_data.append(data)
            # re-encrypt the buckets
            self._encrypt_bucket(node1.data)
            if node1 != node2:
                self._encrypt_bucket(node2.data)
        return deleted_data if deleted_data != [] else None

    def get_public_key(self) -> rsa.RSAPublicKey:
        """
        @return: pb_key: PublicKey, client's public key
        """
        return self.public_key

    # =============================== PROTECTED_METHODS =============================== #

    def _generate_public_key(self) -> rsa.RSAPublicKey:
        """
        @return: pb_key: PublicKey, generates new public key from private key of the client
        """
        return self.private_key.public_key()

    def _assign_to_leaf(self, filename: str) -> int:
        """
        assign given file name to a randomly chosen leaf key
        @param filename: str, filename to be assigned
        @return: leaf_id: int, chosen leaf id
        """
        leaf_id = random.choice(range(self.min_leaf, self.max_leaf + 1))
        self.file_to_leaf[filename] = leaf_id
        return leaf_id

    def _push_down(self, block: Block, node: Node, server: Server) -> [None, str]:
        """
        helper function of Client.flush()
        push given block inside node to the corresponding child in the lower level in the tree.
        if given node is a leaf node, data is deleted from server, and returned

        @param block: Block, block to push down
        @param node: Node, node that holds this block
        @param server: Server, server corresponding to given node
        @return:
        """
        if block is None or node is None:
            raise ValueError(f'{CLIENT} arguments with value None was given to _push_down')
        # if given node is a leaf, there no where to push down to
        if node in server.oram.leaves:
            # print(f'{CLIENT} overflow occurred\ndata={data}', file=sys.stderr)
            return block.data
        # save blocks data to write to lower level
        lid, data = block.leaf_id, block.data
        # if block is empty, just re-encrypt both node's children
        if data == EMPTY_DATA:
            self._decrypt_bucket(node.left.data)
            self._decrypt_bucket(node.right.data)
            self._encrypt_bucket(node.left.data)
            self._encrypt_bucket(node.right.data)
            return
        # clear block
        block.clear()
        # get the correct path, corresponding to the leaf id of the block
        path = server.oram.get_root_path(lid)
        # if the block in not assigned to a leaf path will be None
        if path is None:
            raise ValueError(f'{CLIENT} path is None, leaf id is {lid}')
        # push down the data to the correct child: according to the path from root node to leaf node
        elif node.left.key in path:
            self._decrypt_bucket(node.left.data)
            node.left.data.write_data(data, lid)
            self._encrypt_bucket(node.left.data)
        elif node.right.key in path:
            self._decrypt_bucket(node.right.data)
            node.right.data.write_data(data, lid)
            self._encrypt_bucket(node.right.data)
        else:
            assert False, f'path={path}, lid={lid}, node={node.key} is node leaf? {node.key in server.oram.leaf_keys}'

    def _fill_oram_tree(self, server: Server) -> None:
        """
        encrypts all buckets inside the oram tree of the server
        @param server: Server, registered server
        @return:
        """
        for node in server.oram.nodes:
            self._encrypt_bucket(node.data)

    def _extract_file(self, file: str) -> Tuple[str, str]:
        """
        extracts filename, data from given file. file is in format: <filename>;<data>
        @param file: str, file in the correct format
        @return: filename, data: str,str, extracted filename and its data
        """
        idx = file.find(';')
        if idx == -1:
            raise ValueError(f'{CLIENT} file given was in wrong format')
        # return filename, data
        return file[:idx], file[idx + 1:]

    def _encrypt_bucket(self, bucket: Bucket) -> None:
        """
        encrypts all blocks inside given bucket
        @param bucket: Bucket, bucket to encrypt
        @return:
        """
        if DEBUG:
            return
        for block in bucket.get_array():
            # assert len(block.data) < 512, f'data is {block.data}, bid is {block.bid}'
            file_bytes = block.data.encode()
            lid_bytes = str(block.leaf_id).encode()
            block.set_data(encrypt(self._generate_public_key(), file_bytes))
            block.set_leaf_id(encrypt(self._generate_public_key(), lid_bytes))

    def _decrypt_bucket(self, bucket: Bucket) -> None:
        """
        decrypt all blocks inside given bucket
        @param bucket: Bucket, bucket to decrypt
        @return:
        """
        if DEBUG:
            return
        for block in bucket.get_array():
            # assert len(block.data) >= 512, f'data is {block.data}'
            try:
                block.set_leaf_id(int(decrypt(self.private_key, block.leaf_id).decode('utf-8')))
                block.set_data(decrypt(self.private_key, block.data).decode('utf-8'))
            except (ValueError, TypeError):
                continue

    def _sign_file(self, filename: str, message: str) -> None:
        """
        sign the given message
        @param filename: str, filename corresponds to given data/message
        @param message: str, message to sign over
        @return:
        """
        sig = sign(self.private_key, message.encode())
        self.file_to_sig[filename] = sig


def example_client_read_write():
    """
    example of client read write operations with a registered server
    @return:
    """
    num_data_blocks = 1
    num_files = 1
    server = Server(num_data_blocks)
    client = Client(server)
    with open('./example_file.txt', 'r') as file:
        data = file.readlines()
    i = 0
    while i < num_files:
        client.write(server, f'file{i}', data[i].replace('\n', ''))
        if i % 4 == 0:
            print(f'write progress...{((i + 1) / num_files) * 100}%')
        i += 1
    # print('*** before client.read ***')
    target_files = [f'file{i}' for i in range(num_data_blocks)]
    # nodes = bucket_bfs(server.oram, target_files)
    # print(f'files found in nodes: {[node.key for node in nodes]}')
    count = 0
    for i in range(num_files):
        if client.read(server, f'file{i}'):
            count += 1
        if i % 32 == 0:
            print(f'read progress...{((i + 1) / num_files) * 100}%')
    # print('\n*** after client.read ***')
    print(f'client read successfully {count} files')
    print(f'overwrite {num_files - count} files')
    print(f'files was assigned to {len(set(client.file_to_leaf.values()))} different leaves')

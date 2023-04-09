from cryptography.exceptions import InvalidSignature
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
import random
import os
from typing import List, Union

UNASSIGNED = -1  # leaf key of unassigned block
EMPTY_DATA = '0;0'  # empty data inside block <filename>;<data>
DATA_LEN = 32  # length of data inside the server
KEYS_PATH = './keys'


class Block:
    """
    represents a data unit inside a Bucket instance
    """

    def __init__(self, bid: int, leaf_id: int, data: [str, bytes]):
        """
        @param bid: int, block id of block
        @param leaf_id: int, leaf key this block is assigned to
        @param data: data inside this block
        """
        self.bid: int = bid
        self.leaf_id: [int, bytes] = leaf_id
        self.data: [str, bytes] = data

    def __str__(self):
        return f'Block-{self.bid}'

    def __repr__(self):
        return self.__str__()

    def set_data(self, data: [bytes, str]) -> None:
        """
        sets given data inside this block
        @param data: data to write to this block
        @return:
        """
        self.data = data

    def set_leaf_id(self, leaf_id: int) -> None:
        """
        sets leaf id of this block
        @param leaf_id: new leaf if to assign
        @return:
        """
        self.leaf_id = leaf_id

    def clear(self) -> None:
        """
        clears this block's data
        @return:
        """
        # self.leaf_id = UNASSIGNED
        self.data = EMPTY_DATA

    def is_empty(self) -> bool:
        """
        @return: true if this block is empty, false otherwise
        """
        return self.data == EMPTY_DATA


class Bucket:
    """
    this class represents a Bucket entity inside a Node of an ORAM tree
    """

    def __init__(self, size: int, key: int, leaf_keys: [None, List[int]] = None):
        """
        @param size: size of this bucket. suppose to be log(n), where n is the number of leaves in the tree
        @param key: key of node this bucket is located in
        @param leaf_keys: optional. in not None, blocks inside this bucket will be assigned to leaf keys from this list
            randomly.
        """
        self.size: int = size
        self.key: int = key
        self.idx_pt: int = 0
        # initialize array with empty blocks
        if leaf_keys is None:
            self.array: List[Block] = [Block((key * size) + i, UNASSIGNED, EMPTY_DATA) for i in range(size)]
        else:
            keys = random.choices(list(leaf_keys), k=size)
            self.array: List[Block] = [Block((key * size) + i, keys[i], EMPTY_DATA) for i in range(size)]

    def __str__(self):
        return f'Bucket-{self.key}'

    def __repr__(self):
        return self.__str__()

    def write_data(self, data: [str, bytes], leaf_id: int) -> None:
        """
        write given data into a block inside this bucket
        @param data: data to write into the block
        @param leaf_id: leaf key to set into this block
        @return:
        """
        # reset the index pointer if exceeded bucket size
        if self.idx_pt >= self.size:
            self.idx_pt = 0
        idxs = self.get_available_blocks()
        # if there are no available blocks
        if len(idxs) < 1:
            block = self.array[self.idx_pt]
            # update index pointer
            self.idx_pt += 1
        # write the data on an available block
        else:
            block = self.array[idxs[0]]
        block.set_data(data)
        block.set_leaf_id(leaf_id)

    def get_array(self) -> List[Block]:
        """
        @return: return list of blocks inside this bucket
        """
        return self.array

    def get_available_blocks(self) -> List[int]:
        """
        @return: returns list of indices of empty blocks inside this bucket
        """
        idxs = []
        for i, block in enumerate(self.array):
            if block.is_empty():
                idxs.append(i)
        return idxs


# ============================================ ASYMMETRIC_ENCRYPTION ============================================ #

def generate_key_pair(private_keyname, public_keyname):
    pr_key_file = f'{KEYS_PATH}/{private_keyname}.pem'
    pb_key_file = f'{KEYS_PATH}/{public_keyname}.pem'
    # if keys exists, read and return
    if os.path.exists(pr_key_file) and os.path.exists(pb_key_file):
        with open(pr_key_file, 'rb') as file:
            private_key = serialization.load_pem_private_key(file.read(), password=None, backend=default_backend())
        with open(pb_key_file, 'rb') as file:
            public_key = serialization.load_pem_public_key(file.read(), backend=default_backend())
    # generate new private key
    else:
        # generate key pair
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096, backend=default_backend())
        public_key = private_key.public_key()

        # serialize keys
        pem_pr = private_key.private_bytes(encoding=serialization.Encoding.PEM,
                                           format=serialization.PrivateFormat.PKCS8,
                                           encryption_algorithm=serialization.NoEncryption())
        pem_pb = public_key.public_bytes(encoding=serialization.Encoding.PEM,
                                         format=serialization.PublicFormat.SubjectPublicKeyInfo)
        # write keys
        with open(pr_key_file, 'wb') as file:
            file.write(pem_pr)
        with open(pb_key_file, 'wb') as file:
            file.write(pem_pb)
    return private_key, public_key


def encrypt(key, message: bytes):
    assert isinstance(message, bytes)
    pad = padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None)
    cipher_text = key.encrypt(message, pad)
    return cipher_text


def decrypt(key, message):
    pad = padding.OAEP(mgf=padding.MGF1(algorithm=hashes.SHA256()), algorithm=hashes.SHA256(), label=None)
    plain_text = key.decrypt(message, pad)
    return plain_text


def sign(pr_key, message: bytes) -> bytes:
    signature = pr_key.sign(message,
                            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
                            hashes.SHA256())
    return signature


def verify(pb_key, message: bytes, signature: bytes) -> bool:
    try:
        pb_key.verify(signature,
                      message,
                      padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
                      hashes.SHA256())
    except InvalidSignature:
        return False
    return True


# ============================================ SYMMETRIC_ENCRYPTION ============================================ #
def generate_symmetric_key():
    key_filename = f'{KEYS_PATH}/sym_key.key'
    if os.path.exists(key_filename):
        return load_key(key_filename)
    key = Fernet.generate_key()
    with open(key_filename, 'wb') as output_key:
        output_key.write(key)
    return key


def load_key(key_name):
    if not os.path.exists(f'{KEYS_PATH}/{key_name}'):
        return generate_symmetric_key()
    with open(f'{KEYS_PATH}/{key_name}', 'rb') as key_file:
        return key_file.read()


def encrypt_symm(key, message: Union[str, bytes]):
    if isinstance(message, str):
        byte_msg = message.encode()
    else:
        byte_msg = message
    fernet_key = Fernet(key)
    cipher_txt = fernet_key.encrypt(byte_msg)
    return cipher_txt


def decrypt_symm(key, message):
    fernet_key = Fernet(key)
    plain_txt = fernet_key.decrypt(message)
    return plain_txt


# ============================================ FILE_FUNCTIONS ============================================ #

def dump_file(file_path):
    """
    creates file with lines with length DATA_LEN lines, from data inside given file
    @param file_path: str, path to file to read the data from
    @return:
    """
    with open(file_path, 'r') as file:
        records = file.readlines()
    output_file = open('../client_data_file.txt', 'w')
    for record in records:
        while record != '':
            line = record[:DATA_LEN]
            record = record[DATA_LEN:]
            if len(line) < DATA_LEN:
                break
            output_file.write(line + '\n')
    output_file.close()


def generate_example_file():
    """
    generate an example client data file with 16348 lines, all with length of DATA_LEN
    @return:
    """
    line = 'defghijklmnopqrstuvwxyz012345678'
    with open('./example_file.txt', 'w') as file:
        for i in range(16348):
            file.write(f'{i}-{line[len(str(i)) + 1:]}\n')


if __name__ == '__main__':
    pass

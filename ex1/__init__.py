# import os.path
# import sys
# sys.path.append(os.path.realpath('ex1'))

# the following lines expose items defined in various files when using 'from ex1 import <item>'
from ex1.server import Server
from ex1.client import Client, DEBUG
from ex1.tree import Node, BinaryTree, bucket_bfs
from ex1.utils import Bucket, Block, EMPTY_DATA, UNASSIGNED, sign, verify, generate_key_pair, \
    encrypt, decrypt, load_key, decrypt_symm, encrypt_symm, generate_symmetric_key

# this defines what to import when using 'from ex1 import *'
__all__ = ['Server', 'Client',
           'BinaryTree', 'Node', 'bucket_bfs',
           'Bucket', 'Block', 'UNASSIGNED', 'EMPTY_DATA',
           'sign', 'verify',
           'generate_key_pair', 'encrypt', 'decrypt',
           'load_key', 'generate_symmetric_key', 'encrypt_symm', 'decrypt_symm',
           'DEBUG'
           ]


# from ex1.server import Server
# from ex1.client import *
# from ex1.tree import *
# from ex1.utils import *
# __all__ = ['server', 'client', 'utils', 'tree']

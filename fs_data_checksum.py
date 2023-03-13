import logging, argparse, hashlib, glob
import os.path
from kv import KV
from typing import Generator

class Database:
    DB_NAME = 'checksum.db'
    def __init__(self, root: str):
        self._db = KV(root + '/' + Database.DB_NAME)

    def set_checksum(self, path: str, checksum: str):
        self._db.set_value(path, checksum)

    def get_checksum(self, path: str) -> str:
        return self._db.get_value(path)
    

def calculate_file_checksum(file: str)  -> str:
    BUFFER_SIZE = 100 * 1024 * 1024
    hl = hashlib.md5()
    with open(file, 'rb') as fd:
        while chunk := fd.read(BUFFER_SIZE):
            hl.update(chunk)
    return hl.hexdigest()

def dir_iterator(root: str) -> Generator:
    for path_entry in glob.iglob('**/*', root_dir=root, recursive=True):
        if path_entry!=Database.DB_NAME and os.path.isfile(root+'/'+path_entry):
            yield path_entry


def add(root: str):
    db = Database(root)
    for f in dir_iterator(root):
        if db.get_checksum(f) is None:
            csum = calculate_file_checksum(root+'/'+f)
            print(f'add csum: {f} -> {csum}')
            db.set_checksum(f, csum)

def verify(root: str):
    db = Database(root)
    for f in dir_iterator(root):
        stored_csum = db.get_checksum(f)
        if stored_csum is not None:
            csum = calculate_file_checksum(root+'/'+f)
            if csum != stored_csum:
                print(f'verify csum: {f}  differs! stored={stored_csum} current={csum}')
            else:
                print(f'verify csum: {f}  OK')

# def purge(root: str):
#     pass

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--add', action='store_const', const='True', help='Calculate checksum of files and store in DB. Only adds files not yet in DB')
    parser.add_argument('--verify', action='store_const', const='True', help='Compare file checksums vs stored in DB')
    # parser.add_argument('--purge', action='store_const', const='True', help='Clean DB by deleting entries of files that don\'t exist')
    parser.add_argument('--root', type=str, required=True, help='Root directory of filesystem')
    args = parser.parse_args()

    if args.add:
        add(args.root)
    elif args.verify:
        verify(args.root)
    # elif args.purge:
    #     purge(args.root)
    else:
        logging.error('must specify one of --add, --verify or --purge')

if __name__ == '__main__':
    main()
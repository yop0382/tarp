import os

import tar
from index import index
from multiprocessing import Pool
import uuid
import time
import os
import sys

tar_file_name = sys.argv[3]
directory_to_tar = sys.argv[1]
db_file_path = sys.argv[2]
threads = int(sys.argv[4])

db = index(db_file_path)


def tfile(name):
    btarid = "{0}.btar".format(uuid.uuid4())
    fsize = os.path.getsize(name)

    # Create tar block file
    tar.create_block_from_stream(btarid, tar.create_file_tarinfo_from_stream(name, fsize), open(name, "rb"))
    bfsize = os.path.getsize(btarid)

    # ask where to copy this tar block and register it
    start_offset = db.insert_and_retrieve_new_offset(bfsize, btarid)

    # write block to offset
    tar.write_block_final_tar(tar_file_name, btarid, start_offset)

    # remove temporary tar block
    os.remove(btarid)

    print("file size : {0}, btar size : {1}, start_offset : {2}".format(fsize, bfsize, start_offset, ))


if __name__ == '__main__':
    tfiles = []

    print(directory_to_tar)
    print(tar_file_name)

    for r, d, f in os.walk(directory_to_tar):
        for file in f:
            tfiles.append(os.path.join(r, file))

    start = time.perf_counter()

    with Pool(threads) as p:
        p.map(tfile, tfiles)

    db.close()

    print(f"Completed Execution in {time.perf_counter() - start} seconds")

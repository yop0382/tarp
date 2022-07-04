import os

import tar
from index import index
from multiprocessing import Pool
import uuid
import time
import os
import sys

# Launch Params
# docker
# docker run --rm -v /home/dev/Documents/pycharm-community-2022.1.3:/cache -v /home/dev/:/output -it tarp:latest /cache /output/.output.db /output/output.tar 1
# Python
# python3 main.py /home/dev/Documents/pycharm-community-2022.1.3/ output.db output.tar 2

tar_file_name = sys.argv[3]
directory_to_tar = sys.argv[1]
db_file_path = sys.argv[2]
threads = int(sys.argv[4])


# Thread method
def tfile(name):
    # tar.add_file_to_archive(name, tar_file_name)
    tar.add_file_to_archive_stream(name, tar_file_name)


if __name__ == '__main__':
    tfiles = []

    print("Directory to TAR : {0}, Tar output : {1}, Database Index Path : {2}, Thread : {3}".format(directory_to_tar,
                                                                                                     tar_file_name,
                                                                                                     db_file_path,
                                                                                                     threads))

    for r, d, f in os.walk(directory_to_tar):
        for file in f:
            tfiles.append(os.path.join(r, file))

    start = time.perf_counter()

    with Pool(threads) as p:
        p.map(tfile, tfiles)

    print(f"Completed Execution in {time.perf_counter() - start} seconds")

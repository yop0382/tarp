import os
from concurrent.futures import ThreadPoolExecutor

import tar
from index import index
from multiprocessing import Pool
import uuid
import time
import os
import sys
from threading import Lock

# Launch Params
# docker
# docker run --rm -v /home/dev/Documents/pycharm-community-2022.1.3:/cache -v /home/dev/:/output -it tarp:latest /cache /output/.output.db /output/output.tar 1
# Python
# python3 main.py /home/dev/Documents/pycharm-community-2022.1.3/ output.db output.tar 2


directory_to_tar = sys.argv[1]
output_write_path = sys.argv[2]
tar_file_name = sys.argv[3]
threads = int(sys.argv[4])


# Thread method
def tfile(name, flock):
    print(name)
    # tar.add_file_to_archive(name, tar_file_name)
    tar.add_file_to_archive_stream(name, tar_file_name, output_write_path, flock)


if __name__ == '__main__':
    tfiles = []

    print("Directory to TAR : {0}, Tar output : {1}, Output Path : {2}, Thread : {3}".format(directory_to_tar,
                                                                                             tar_file_name,
                                                                                             output_write_path,
                                                                                             threads))

    for r, d, f in os.walk(directory_to_tar):
        for file in f:
            tfiles.append(os.path.join(r, file))

    start = time.perf_counter()

    lock = Lock()

    with ThreadPoolExecutor(threads) as p:
        _ = [p.submit(tfile, f, lock) for f in tfiles]
        # p.map(tfile, tfiles)

    print(f"Completed Execution in {time.perf_counter() - start} seconds")

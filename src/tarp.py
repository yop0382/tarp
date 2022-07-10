# function file for mt with tar file
import os
import utils
import tarfile

import redis_lock


class tarp(object):

    def __init__(self, tar_file_name, output_write_path, flock, chunk_size=4096):
        self.tar_file_name = tar_file_name
        self.output_write_path = output_write_path
        self.flock = flock
        self.chunk_size = chunk_size

    def __enter__(self):
        return self

    def __exit__(self):
        pass

    # STREAM METHOD

    # def add_stream_to_archive_stream(self, file_name, file_stream, tar_file_name, output_write_path):
    #     self.write_block_final_tar_stream(file_name, file_stream, "{0}/{1}".format(output_write_path, tar_file_name))

    def add_file_to_archive_stream(self, file_name):
        file_stream = open(file_name, "rb")
        self.write_block_final_tar_stream(file_name, file_stream,
                                          "{0}/{1}".format(self.output_write_path, self.tar_file_name),
                                          self.flock)

    def write_block_final_tar_stream(self, file_name, file_stream, tar_file_name, flock):
        # Get Stream Size
        file_stream.seek(0, 2)
        file_size = file_stream.tell()
        file_stream.seek(0)

        # Tar Info
        tarinfo = tarfile.TarInfo(file_name)
        tarinfo.size = file_size

        buf = tarinfo.tobuf(tarfile.GNU_FORMAT, tarfile.ENCODING, "surrogateescape")

        # Lock Python Threading
        # with flock:
        with redis_lock.Lock(flock, os.path.basename(tar_file_name)):
            # Write to tar
            output = open(tar_file_name, "ab")

            # Write header
            output.write(buf)

            # Write data
            utils.copyfileobj(file_stream, output, None, self.chunk_size)

            # Padding
            blocks, remainder = divmod(file_size, 512)
            if remainder > 0:
                output.write((b"\0" * (512 - remainder)))
                blocks += 1

            # Release
            output.close()

        file_stream.close()
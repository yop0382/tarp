# function file for mt with tar file
import copy
import os
import shutil
import tarfile
import uuid
from pathlib import Path
from filelock import FileLock


# STREAM METHOD

def add_stream_to_archive_stream(file_name, file_stream, tar_file_name, output_write_path):
    write_block_final_tar_stream(file_name, file_stream, "{0}/{1}".format(output_write_path, tar_file_name))


def add_file_to_archive_stream(file_name, tar_file_name, output_write_path):
    file_stream = open(file_name, "rb")
    write_block_final_tar_stream(file_name, file_stream, "{0}/{1}".format(output_write_path, tar_file_name))


def write_block_final_tar_stream(file_name, file_stream, tar_file_name):
    # Get Stream Size
    file_stream.seek(0, 2)
    file_size = file_stream.tell()
    file_stream.seek(0)

    # Tar Info
    tarinfo = tarfile.TarInfo(file_name)
    tarinfo.size = file_size

    buf = tarinfo.tobuf(tarfile.GNU_FORMAT, tarfile.ENCODING, "surrogateescape")

    # Lock
    lock = FileLock("{0}.lock".format(tar_file_name))
    lock.acquire(timeout=-1)

    # Write to tar
    output = open(tar_file_name, "ab")

    # Write header
    output.write(buf)

    # Write data
    copyfileobj(file_stream, output, None, None)

    # Padding
    blocks, remainder = divmod(file_size, 512)
    if remainder > 0:
        output.write((b"\0" * (512 - remainder)))
        blocks += 1

    # Release
    output.close()
    lock.release()

    file_stream.close()


# FILE METHOD

def add_file_to_archive(filename_to_add, tar_file_name):
    # Database for Tar parralellism
    # db = index(db_index_object)

    btarid = "{0}.btar".format(uuid.uuid4())
    fsize = os.path.getsize(filename_to_add)

    # Create tar block file
    create_block_from_stream(btarid, create_file_tarinfo_from_stream(filename_to_add, fsize),
                             open(filename_to_add, "rb"))

    # write block to offset
    write_block_final_tar(tar_file_name, btarid, None)

    # remove temporary tar block
    os.remove(btarid)

    # db.close()

    print("filename : {3}, file size : {0}, btar size : {1}, start_offset : {2}".format(fsize, None, None,
                                                                                        filename_to_add))


def create_file_tarinfo_from_stream(btarid, size):
    tarinfo = tarfile.TarInfo(btarid)
    tarinfo.size = size

    return tarinfo


def create_block_from_stream(tar_name, tarinfo, filestream):
    with tarfile.open(tar_name, "a") as f:
        f.format = tarfile.GNU_FORMAT
        f.addfile(tarinfo, filestream)
        f.closed = True
        f.fileobj.close()


def write_block_final_tar(tar_file_name, block_file, start_offset):
    lock = FileLock("{0}.lock".format(tar_file_name))

    lock.acquire(timeout=-1)

    path = Path(tar_file_name)

    if path.is_file():
        pass
    else:
        output = open(tar_file_name, "w")
        output.close()

    output = open(tar_file_name, "ab")
    # output.seek(start_offset)
    fblock = open(block_file, "rb")
    copyfileobj(fblock, output, None, None)
    fblock.close()
    output.close()
    lock.release()


def copyfileobj(src, dst, length=None, exception=OSError, bufsize=None):
    """Copy length bytes from fileobj src to fileobj dst.
       If length is None, copy the entire content.
    """
    bufsize = bufsize or 16 * 1024
    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst, bufsize)
        return

    blocks, remainder = divmod(length, bufsize)
    for b in range(blocks):
        buf = src.read(bufsize)
        if len(buf) < bufsize:
            raise exception("unexpected end of data")
        dst.write(buf)

    if remainder != 0:
        buf = src.read(remainder)
        if len(buf) < remainder:
            raise exception("unexpected end of data")
        dst.write(buf)
    return

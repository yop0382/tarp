# function file for mt with tar file
import os
import shutil
import tarfile
import uuid
from pathlib import Path

from index import index


def add_file_to_archive(filename_to_add, tar_file_name, db_index_object):
    # Database for Tar parralellism
    db = index(db_index_object)

    btarid = "{0}.btar".format(uuid.uuid4())
    fsize = os.path.getsize(filename_to_add)

    # Create tar block file
    create_block_from_stream(btarid, create_file_tarinfo_from_stream(filename_to_add, fsize),
                             open(filename_to_add, "rb"))
    bfsize = os.path.getsize(btarid)

    # ask where to copy this tar block and register it
    start_offset = db.insert_and_retrieve_new_offset(bfsize, btarid)

    # write block to offset
    write_block_final_tar(tar_file_name, btarid, start_offset)

    # remove temporary tar block
    os.remove(btarid)

    db.close()

    print("filename : {3}, file size : {0}, btar size : {1}, start_offset : {2}".format(fsize, bfsize, start_offset,
                                                                                        filename_to_add))


def create_file_tarinfo_from_stream(tfilename, size):
    tarinfo = tarfile.TarInfo(tfilename)
    tarinfo.size = size

    return tarinfo


def create_block_from_stream(tar_name, tarinfo, filestream):
    with tarfile.open(tar_name, "a") as f:
        f.format = tarfile.GNU_FORMAT

        f.addfile(tarinfo, filestream)

        f.closed = True
        f.fileobj.close()


def write_block_final_tar(tar_file_name, block_file, start_offset):
    path = Path(tar_file_name)

    if path.is_file():
        pass
    else:
        output = open(tar_file_name, "w")
        output.close()

    output = open(tar_file_name, "r+b")

    output.seek(start_offset)

    fblock = open(block_file, "rb")

    copyfileobj(fblock, output, None, None)

    fblock.close()
    output.close()


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

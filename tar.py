# function file for mt with tar file
import copy
import errno
import os
import shutil
import tarfile
import time
import uuid
from pathlib import Path
from threading import Lock


# STREAM METHOD

def add_stream_to_archive_stream(file_name, file_stream, tar_file_name, output_write_path):
    write_block_final_tar_stream(file_name, file_stream, "{0}/{1}".format(output_write_path, tar_file_name))


def add_file_to_archive_stream(file_name, tar_file_name, output_write_path, flock):
    file_stream = open(file_name, "rb")
    write_block_final_tar_stream(file_name, file_stream, "{0}/{1}".format(output_write_path, tar_file_name), flock)


def write_block_final_tar_stream(file_name, file_stream, tar_file_name, flock):
    # Get Stream Size
    file_stream.seek(0, 2)
    file_size = file_stream.tell()
    file_stream.seek(0)

    # Tar Info
    tarinfo = tarfile.TarInfo(file_name)
    tarinfo.size = file_size

    buf = tarinfo.tobuf(tarfile.GNU_FORMAT, tarfile.ENCODING, "surrogateescape")

    # Lock
    with flock:
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

    file_stream.close()


def predict_size(file_size, buf_size):
    blocks, remainder = divmod(file_size, 512)
    if remainder > 0:
        blocks += 1
    return (blocks * 512) + buf_size


def append_chunk_block_size(tar_file_name, psize):
    chunk_size = 104_857_600
    if psize > chunk_size:
        chunk_size = (psize + chunk_size)

    with open(tar_file_name, 'ab') as file:
        file.write(b'\0' * chunk_size)


def lockfile(target, link, timeout=300):
    global lock_owner
    poll_time = 0.05
    while timeout > 0:
        try:
            os.link(target, link)
            print("Lock acquired")
            lock_owner = True
            break
        except OSError as err:
            print(err)
            if err.errno == errno.EEXIST:
                print("Lock unavailable. Waiting for 10 seconds...")
                time.sleep(poll_time)
                timeout -= poll_time
            else:
                raise err
    else:
        print("Timed out waiting for the lock.")


def releaselock(link):
    try:
        if lock_owner:
            os.unlink(link)
            print("File unlocked")
    except OSError:
        print("Error:didn't possess lock.")


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

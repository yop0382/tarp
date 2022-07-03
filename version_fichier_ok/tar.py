# function file for mt with tar file
import shutil
import tarfile
from pathlib import Path
import copy


def create_file_tarinfo_from_stream(tfilename, size):
    tarinfo = tarfile.TarInfo(tfilename)
    tarinfo.size = size

    return tarinfo


def divider_remainder(filesize):
    return divmod(filesize, 512)


# Size prediction to optimize buffer copy during process
def btar_size_predict(filesize):
    (blocks, remainder) = divider_remainder(filesize)
    if remainder > 0:
        blocks += 1
    final_size = (blocks * 512) + 512
    return final_size


# def create_dir_tarinfo(tdirname):
#     tarinfo = tarfile.TarInfo(tdirname)
#     tarinfo.type = tarfile.DIRTYPE
#
#     return tarinfo


# def create_block_from_tarinfo(tar_name, tarinfo):
#     with tarfile.open(tar_name, "a") as f:
#         f.format = tarfile.GNU_FORMAT
#
#         f.addfile(tarinfo)
#
#         f.closed = True
#         f.fileobj.close()


def create_block_from_stream(tar_name, tarinfo, filestream):
    with tarfile.open(tar_name, "a") as f:
        f.format = tarfile.GNU_FORMAT

        f.addfile(tarinfo, filestream)

        f.closed = True
        f.fileobj.close()


# with tarfile.open(tar_name, "a") as f:
#     f.format = tarfile.GNU_FORMAT
#     stream.seek(0)
#     f.addfile(tarinfo, stream)
#     stream.close()
#
#     return f

# f.closed = True
# f.fileobj.close()


def write_block_stream_final_tar(tar_file_name, file_stream, file_size, tarinfo, predicted_size, start_offset, btarid):
    path = Path(tar_file_name)

    if path.is_file():
        pass
    else:
        output = tarfile.open(tar_file_name, "w")
        output.format = tarfile.GNU_FORMAT
        output.close()

    output = open(tar_file_name, "r+b")

    output.seek(start_offset)

    # Stream Copy

    # Set GNUTAR with 1
    tarinfo = copy.copy(tarinfo)
    tarinfo_buf = tarinfo.tobuf(1, "utf-8", "surrogateescape")
    # Copy Header
    output.write(tarinfo_buf)
    # Copy file
    copyfileobj(file_stream, output, file_size, None)

    # file Copy
    with tarfile.open(tar_file_name, "a") as f:
        f.format = tarfile.GNU_FORMAT

        f.addfile(tarinfo)

        f.closed = True
        f.fileobj.close()

    file_stream.close()

    # Add Footer
    (blocks, remainder) = divider_remainder(file_size)
    if remainder > 0:
        output.write(b"\0" * (512 - remainder))

    # fblock.close()
    fsize = output.tell()
    output.close()

    return fsize


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

    while True:
        chunk = fblock.read(4096)
        output.write(chunk)
        if not chunk: break

    fblock.close()
    output.close()

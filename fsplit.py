import argparse
import os

READSZ = 1048576
MIN_CHUNK_SZ = 10485760
MIN_CHUNKS_ALLOWED = 2
MAX_CHUNKS_ALLOWED = 999


def get_args():
    parser = argparse.ArgumentParser(description="Splits a big file into chunks. Leaves original file intact")
    parser.add_argument(
        '-n', '--numchunks',
        metavar='numchunks', type=int, default=2,
        help="Number of chunks into which the file will be split, Default = 2"
    )
    parser.add_argument('filename', type=str, help="Name of the file you want to split")
    parser.add_argument(
        '-o', '--outputdir', type=str, metavar='outputdir',
        help='Output path. Default is same dir the source file is in.'
    )
    arguments = parser.parse_args()
    return arguments


def fill_chunk(src_file, dest_file, chunk_length):
    """
    Fill a chunk file with a chunk sized amount of bytes from the source
    :param src_file: file descriptor
    :param dest_file: file descriptor
    :param chunk_length: int
    :return:
    """
    rw_amount = 0
    if not chunk_length:
        raise Exception()

    bytes_left = chunk_length
    while bytes_left > 0:
        if bytes_left >= READSZ:
            rw_amount = READSZ
        elif bytes_left < READSZ:
            rw_amount = bytes_left
        bytes_left -= os.write(dest_file, os.read(src_file, rw_amount))
        print('.', end='')


def calculate_chunk_sizes(file_name, num_chunks):
    """
    Set the chunk file size to file_size // num_chunks. Last chunk may be slightly larger than the other chunks
    due to integer division.
    :param file_name: str
    :param num_chunks: int
    :return: tuple(int, int)
    """
    stat_info = os.stat(file_name)
    file_size = stat_info.st_size
    chunk_length = file_size // num_chunks
    last_chunk_length = file_size - ((num_chunks - 1) * chunk_length)
    return chunk_length, last_chunk_length


def process_file_to_chunks(file_name, num_chunks, outputdir=None):
    """
    Start making chunkfiles
    :param file_name: str
    :param num_chunks: int
    :param outputdir: str
    :return:
    """
    chunk_size, last_chunk_size = calculate_chunk_sizes(file_name, num_chunks)
    print('{} chunks will be at least {} bytes'.format(num_chunks - 1, chunk_size))
    print('Last chunk will be {} bytes'.format(last_chunk_size))

    src_file = os.open(args.filename, os.O_RDONLY | os.O_BINARY)
    for chunk in range(1, num_chunks + 1):
        # set the chunk's file name

        if outputdir:
            chunk_path, chunk_file_name = os.path.split(file_name)
            final_file_name = os.path.join(outputdir, chunk_file_name)
        else:
            final_file_name = file_name
        chunk_name = "{filename}.CHUNK{chunknum:03d}".format(filename=final_file_name, chunknum=chunk)
        print('Creating file {}'.format(chunk_name), end='')
        # open the chunk file for writing, truncate it if it exists, create it if it doesn't
        chunk_file = os.open(chunk_name, os.O_BINARY | os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        ch_size = last_chunk_size if chunk == num_chunks else chunk_size
        # write it full of bytes from the requisite chunk of the source file
        fill_chunk(src_file, chunk_file, ch_size)
        os.close(chunk_file)
        print("Done")
    os.close(src_file)


def validate_args(chunk_count, file_name, output_dir):
    """
    Make sure the arguments are reasonable, especially paths and files. Raise appropriate errors and warnings
    :param chunk_count: int
    :param file_name: str
    :param output_dir: str
    :return:
    """
    if chunk_count < MIN_CHUNKS_ALLOWED:
        raise ValueError("--numchunks must be {} or greater".format(MIN_CHUNKS_ALLOWED))
    if chunk_count > MAX_CHUNKS_ALLOWED:
        raise ValueError("Maximum number of chunks allowed is {}".format(MAX_CHUNKS_ALLOWED))

    if not os.path.exists(file_name):
        raise OSError("File {} does not exist".format(file_name))
    if not os.path.isfile(file_name):
        raise OSError("{} is not a file".format(file_name))
    if not os.access(file_name, os.R_OK):
        raise OSError("You haven't permissions to read {}".format(file_name))

    # Testing for max chunk count comes after the file tests, because we may not be able to stat the file
    # if a chunk is going to end up being less than a 10 megabytes user is asking for too many chunks
    if os.stat(file_name).st_size // chunk_count < MIN_CHUNK_SZ:
        print("Chunks will be < 10MB. You might be asking for too many chunks")

    if output_dir:
        if not os.path.exists(output_dir):
            raise OSError("Path {} does not exist".format(output_dir))
        if not os.path.isdir(output_dir):
            raise OSError("{} is not a directory".format(output_dir))
        if not os.access(output_dir, os.W_OK):
            raise OSError("You haven't permissions to write to {}".format(output_dir))


if __name__ == '__main__':
    args = get_args()
    validate_args(args.numchunks, args.filename, args.outputdir)
    process_file_to_chunks(args.filename, args.numchunks, outputdir=args.outputdir)

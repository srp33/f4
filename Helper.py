import mmap

def open_read_file(file_path, file_extension=""):
    the_file = open(file_path + file_extension, 'rb')
    return mmap.mmap(the_file.fileno(), 0, prot=mmap.PROT_READ)

def read_int_from_file(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return int(the_file.read().rstrip())

def is_missing_value(value):
    return value == b"" or value == b"NA"

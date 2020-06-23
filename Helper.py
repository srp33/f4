import mmap

def parse_data_coords(line_indices, coords_file, coords_file_max_length):
    data_coords = []
    out_dict = {}

    for index in line_indices:
        start_pos = index * (coords_file_max_length + 1)
        next_start_pos = start_pos + coords_file_max_length + 1
        further_next_start_pos = next_start_pos + coords_file_max_length + 1

        # See if we already have cached the start position.
        if index in out_dict:
            data_start_pos = out_dict[index]
        # If not, retrieve the start position from the cc file and then cache it.
        else:
            data_start_pos = int(coords_file[start_pos:next_start_pos].rstrip())
            out_dict[index] = data_start_pos

        # See if we already have cached the end position.
        if (index + 1) in out_dict:
            data_end_pos = out_dict[index + 1]
        # If not, retrieve the end position from the cc file and then cache it.
        else:
            data_end_pos = int(coords_file[next_start_pos:further_next_start_pos].rstrip())
            out_dict[index + 1] = data_end_pos

        data_coords.append([data_start_pos, data_end_pos])

    return data_coords

def parse_data_values(start_offset, segment_length, data_coords, str_like_object):
    start_pos = start_offset * segment_length

    for coords in data_coords:
        yield str_like_object[(start_pos + coords[0]):(start_pos + coords[1])]

def read_string_from_file(file_path, file_extension=""):
    with open(file_path + file_extension, 'rb') as the_file:
        return the_file.read().rstrip()

def read_strings_from_file(file_path, file_extension=""):
    strings = []
    with open(file_path + file_extension, 'rb') as the_file:
        for line in the_file:
            strings.append(line.rstrip(b"\n"))
    return strings

def read_int_from_file(file_path, file_extension=""):
    return int(read_string_from_file(file_path, file_extension))

def open_read_file(file_path, file_extension=""):
    the_file = open(file_path + file_extension, 'rb')
    return mmap.mmap(the_file.fileno(), 0, prot=mmap.PROT_READ)

def is_missing_value(value):
    return value == b"" or value == b"NA"

import f4py
import os
import string
#TODO
import zstandard

class CompressionHelper:
#    def _save_training_dict(training_set, f4_file_path, compression_level, num_processes):
#        training_list = sorted(list(training_set))
#        training_num_chars = sum([len(x) for x in training_set])
#
#        training_dict = zstandard.train_dictionary(training_num_chars, training_list, level=compression_level, threads=num_processes)
#
#        with open(CompressionHelper._get_training_dict_file_path(f4_file_path), "wb") as dict_file:
#            dict_file.write(training_dict.as_bytes())

    def _get_compressor(f4_file_path, compression_level):
        if compression_level == None:
            return None

#        training_dict_file_path = CompressionHelper._get_training_dict_file_path(f4_file_path)
#
#        if os.path.exists(training_dict_file_path):
#            with open(training_dict_file_path, 'rb') as dict_file:
#                training_dict = zstandard.ZstdCompressionDict(dict_file.read())
#                return zstandard.ZstdCompressor(dict_data = training_dict, level = compression_level)

        return zstandard.ZstdCompressor(level = compression_level)

#    def _get_decompressor(f4_file_path):
#        level = CompressionHelper._get_level(f4_file_path)
#
#        if level != None:
#            training_dict_file_path = CompressionHelper._get_training_dict_file_path(f4_file_path)
#
#            if os.path.exists(training_dict_file_path):
#                with open(training_dict_file_path, "rb") as dict_file:
#                    training_dict = zstandard.ZstdCompressionDict(dict_file.read())
#                    return zstandard.ZstdDecompressor(dict_data=training_dict)
#
#            return zstandard.ZstdDecompressor()
#
#        return None

    def _get_level_file_path(f4_file_path):
        return f"{f4_file_path}.cmpl"

    def _save_level_file(f4_file_path, compression_level):
        f4py.write_str_to_file(CompressionHelper._get_level_file_path(f4_file_path), str(compression_level).encode())

    def _get_level(f4_file_path):
        file_path = CompressionHelper._get_level_file_path(f4_file_path)

        if not os.path.exists(file_path):
            return None

        raw_level = f4py.read_str_from_file(CompressionHelper._get_level_file_path(f4_file_path))

        if raw_level == b"None":
            return None
        return int(raw_level)

#    def _get_training_dict_file_path(f4_file_path):
#        return f"{f4_file_path}.cmpd"

#    def get_compression_characters(num_unique_values):
#        #single_characters = [x.encode() for x in list(string.digits + string.ascii_lowercase + string.ascii_uppercase)]
#        # First 255 UTF-8 characters, exclude newline character
#        #single_characters = [bytes((i,)) for i in range(256) if i != 10]
#        single_characters = [bytes((i,)) for i in range(256)]
#
#        compression_characters = []
#
#        for i in single_characters:
#            compression_characters.append(i)
#
#            if len(compression_characters) == num_unique_values:
#                return compression_characters
#
#        for i in single_characters:
#            for j in single_characters:
#                compression_characters.append(i + j)
#
#                if len(compression_characters) == num_unique_values:
#                    return compression_characters
#
#        for i in single_characters:
#            for j in single_characters:
#                for k in single_characters:
#                    compression_characters.append(i + j + k)
#
#                    if len(compression_characters) == num_unique_values:
#                        return compression_characters
#
#        for i in single_characters:
#            for j in single_characters:
#                for k in single_characters:
#                    for l in single_characters:
#                        compression_characters.append(i + j + k + l)
#
#                        if len(compression_characters) == num_unique_values:
#                            return compression_characters

from bitarray import bitarray
from bitarray import frozenbitarray
import bitstring
import datetime
import lz4.frame as lz4cmpr
import pybase64
import random
import snappy
import sys
import zstandard
import zlib

#number_bits = 8
#number_bits = 1000
number_bits = 1000000
#number_iterations = 10
number_iterations = 2

charset = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_'

#https://stackoverflow.com/questions/5940416/compress-a-series-of-1s-and-0s-into-the-shortest-possible-ascii-string
def encode(bin_string):
    # Split the string of 1s and 0s into lengths of 6.
    chunks = [bin_string[i:i+6] for i in range(0, len(bin_string), 6)]
    # Store the length of the last chunk so that we can add that as the last bit
    # of data so that we know how much to pad the last chunk when decoding.
    last_chunk_length = len(chunks[-1])
    # Convert each chunk from binary into a decimal
    decimals = [int(chunk, 2) for chunk in chunks]
    # Add the length of our last chunk to our list of decimals.
    decimals.append(last_chunk_length)
    # Produce an ascii string by using each decimal as an index of our charset.
    ascii_string = ''.join([charset[i] for i in decimals])

    return ascii_string

def decode(ascii_string):
    # Convert each character to a decimal using its index in the charset.
    decimals = [charset.index(char) for char in ascii_string]
    # Take last decimal which is the final chunk length, and the second to last
    # decimal which is the final chunk, and keep them for later to be padded
    # appropriately and appended.
    last_chunk_length, last_decimal = decimals.pop(-1), decimals.pop(-1)
    # Take each decimal, convert it to a binary string (removing the 0b from the
    # beginning, and pad it to 6 digits long.
    bin_string = ''.join([bin(decimal)[2:].zfill(6) for decimal in decimals])
    # Add the last decimal converted to binary padded to the appropriate length
    bin_string += bin(last_decimal)[2:].zfill(last_chunk_length)

    return bin_string.encode()

length_dict = {"encode": [], "pybase64": [], "zlib": [], "zstandard1": [], "zstandard22": [], "lz4": [], "snappy": [], "bitstring": [], "bitarray": []}
compression_time_dict = {"encode": [], "pybase64": [], "zlib": [], "zstandard1": [], "zstandard22": [], "lz4": [], "snappy": [], "bitstring": [], "bitarray": []}
decompression_time_dict = {"encode": [], "pybase64": [], "zlib": [], "zstandard1": [], "zstandard22": [], "lz4": [], "snappy": [], "bitstring": [], "bitarray": []}

for i in range(number_iterations):
    bin_string = b''
    options = [b"0", b"1"]
    random.seed(i)

    for i in range(number_bits):
        random.shuffle(options)
        bin_string += options[0]

    bin_string = frozenbitarray(bin_string.decode()).tobytes()
    print(f"Size after bits to bytes: {len(bin_string)}")

#    start = datetime.datetime.now()
#    compressed = encode(bin_string)
#    length_dict["encode"].append(len(compressed))
#    end = datetime.datetime.now()
#    compression_time_dict["encode"].append((end - start).total_seconds())
#    decompressed = decode(compressed)
#    end2 = datetime.datetime.now()
#    decompression_time_dict["encode"].append((end2 - start).total_seconds())

#    start = datetime.datetime.now()
#    compressed = pybase64.b64encode(bin_string)
#    length_dict["pybase64"].append(len(compressed))
#    end = datetime.datetime.now()
#    compression_time_dict["pybase64"].append((end - start).total_seconds())
#    decompressed = pybase64.b64decode(compressed, validate=True)
#    end2 = datetime.datetime.now()
#    decompression_time_dict["pybase64"].append((end2 - start).total_seconds())

    #https://pythonshowcase.com/question/how-do-i-compress-a-rather-long-binary-string-in-python-so-that-i-will-be-able-to-access-it-later
    start = datetime.datetime.now()
    compressed = zlib.compress(bin_string)
    length_dict["zlib"].append(len(compressed))
    end = datetime.datetime.now()
    compression_time_dict["zlib"].append((end - start).total_seconds())
    decompressed = zlib.decompress(compressed)
    end2 = datetime.datetime.now()
    decompression_time_dict["zlib"].append((end2 - start).total_seconds())

    start = datetime.datetime.now()
    compressed = zstandard.compress(bin_string, level = 1)
    length_dict["zstandard1"].append(len(compressed))
    end = datetime.datetime.now()
    compression_time_dict["zstandard1"].append((end - start).total_seconds())
    decompressed = zstandard.decompress(compressed)
    end2 = datetime.datetime.now()
    decompression_time_dict["zstandard1"].append((end2 - start).total_seconds())

    start = datetime.datetime.now()
    compressed = zstandard.compress(bin_string, level = 10)
    length_dict["zstandard22"].append(len(compressed))
    end = datetime.datetime.now()
    compression_time_dict["zstandard22"].append((end - start).total_seconds())
    decompressed = zstandard.decompress(compressed)
    end2 = datetime.datetime.now()
    decompression_time_dict["zstandard22"].append((end2 - start).total_seconds())

    start = datetime.datetime.now()
    compressed = lz4cmpr.compress(bin_string)
    length_dict["lz4"].append(len(compressed))
    end = datetime.datetime.now()
    compression_time_dict["lz4"].append((end - start).total_seconds())
    decompressed = lz4cmpr.decompress(compressed)
    end2 = datetime.datetime.now()
    decompression_time_dict["lz4"].append((end2 - start).total_seconds())

    start = datetime.datetime.now()
    compressed = snappy.compress(bin_string)
    length_dict["snappy"].append(len(compressed))
    end = datetime.datetime.now()
    compression_time_dict["snappy"].append((end - start).total_seconds())
    decompressed = snappy.decompress(compressed)
    end2 = datetime.datetime.now()
    decompression_time_dict["snappy"].append((end2 - start).total_seconds())

#    start = datetime.datetime.now()
#    compressed = bitstring.BitArray(bin=bin_string.decode()).tobytes()
#    length_dict["bitstring"].append(len(compressed))
#    end = datetime.datetime.now()
#    compression_time_dict["bitstring"].append((end - start).total_seconds())
#    decompressed = bitstring.BitArray(bytes=compressed).bin.encode()
#    end2 = datetime.datetime.now()
#    decompression_time_dict["bitstring"].append((end2 - start).total_seconds())

#    start = datetime.datetime.now()
#    compressed = frozenbitarray(bin_string.decode()).tobytes()
#    length_dict["bitarray"].append(len(compressed))
#    end = datetime.datetime.now()
#    compression_time_dict["bitarray"].append((end - start).total_seconds())
#    decompressor = bitarray()
#    decompressor.frombytes(compressed)
#    decompressed = decompressor.to01().encode()
#    end2 = datetime.datetime.now()
#    decompression_time_dict["bitarray"].append((end2 - start).total_seconds())

for compression_method, lengths in length_dict.items():
    if len(lengths) == 0:
        continue

    print(compression_method)
    print(f"Mean compressed size: {sum(lengths) / len(lengths)}")
    print(f"Mean compression time: {sum(compression_time_dict[compression_method]) / len(compression_time_dict[compression_method])}")
    print(f"Mean decompression time: {sum(decompression_time_dict[compression_method]) / len(decompression_time_dict[compression_method])}")

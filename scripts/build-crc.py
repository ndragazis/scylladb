#!/usr/bin/env python

import os
import re
import zlib
import pathlib
import argparse
from enum import Enum, StrEnum, auto


class ChecksumType(Enum):
    CRC32 = auto()
    ADLER32 = auto()


class VersionType(StrEnum):
    KA = auto()
    LA = auto()
    MC = auto()
    MD = auto()
    ME = auto()


def calculate_checksum(buf, checksum_type):
    if checksum_type == ChecksumType.CRC32:
        return zlib.crc32(buf)
    elif checksum_type == ChecksumType.ADLER32:
        return zlib.adler32(buf)
    else:
        raise ValueError(f"Unsupported checksum type {checksum_type}")


def is_compressed(sstable_dir):
    regex = re.compile(r'.*-CompressionInfo\.db$')
    for filepath in os.listdir(sstable_dir):
        if regex.match(filepath):
            return True
    return False


def get_version(datafile):
    return datafile.split('-')[0]


def get_crc_file(sstable_dir):
    regex = re.compile(r'.*-CRC\.db$')
    for filepath in os.listdir(sstable_dir):
        if regex.match(filepath):
            return f"{sstable_dir}/{filepath}"
    return None


def read_chunk_size(crc_file):
    with open(crc_file, "rb") as f:
        buf = f.read(4)
        if len(buf) < 4:
            raise ValueError("Buffer size mismatch")
        return int.from_bytes(buf, byteorder='big')


def main():
    prog_description = (
    """
    Helper script to re-build corrupted checksum file (CRC.db) for uncompressed SSTables.
    """)
    parser = argparse.ArgumentParser(
            prog="build-crc",
            description=prog_description)
    parser.add_argument("filename", help="Path to SSTable data file.")
    args = parser.parse_args()

    filename = os.path.abspath(args.filename)
    if not os.path.exists(filename):
        raise ValueError(f"File `{args.filename}' not found.")

    sstable_dir, datafile = os.path.split(filename)
    if is_compressed(sstable_dir):
        raise ValueError("Compressed SSTables are not supported.")

    version = get_version(datafile)
    checksum_type = ChecksumType.CRC32 if version >= VersionType.MC else ChecksumType.ADLER32
    crc_file = get_crc_file(sstable_dir)
    if not crc_file:
        raise RuntimeError("CRC file not found.")
    chunk_size = read_chunk_size(crc_file)
    print(f"Chunk size: {chunk_size}")

    with open(filename, "rb") as dataf, open(crc_file, "wb") as crcf:
        crcf.write(chunk_size.to_bytes(4, byteorder='big'))
        while True:
            chunk = dataf.read(chunk_size)
            if not chunk: break
            print(f"Read {len(chunk)} bytes")
            checksum = calculate_checksum(chunk, checksum_type)
            print(f"checksum={checksum}")
            crcf.write(checksum.to_bytes(4, byteorder='big'))


if __name__ == "__main__":
    main()

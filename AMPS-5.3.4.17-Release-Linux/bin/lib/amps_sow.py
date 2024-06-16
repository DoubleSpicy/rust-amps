###############################################################################
##
# Copyright (c) 2012-2022 60East Technologies Inc., All Rights Reserved.
##
# This computer software is owned by 60East Technologies Inc. and is
# protected by U.S. copyright laws and other laws and by international
# treaties.  This computer software is furnished by 60East Technologies
# Inc. pursuant to a written license agreement and may be used, copied,
# transmitted, and stored only in accordance with the terms of such
# license agreement and with the inclusion of the above copyright notice.
# This computer software or any other copies thereof may not be provided
# or otherwise made available to any other person.
##
# U.S. Government Restricted Rights.  This computer software: (a) was
# developed at private expense and is in all respects the proprietary
# information of 60East Technologies Inc.; (b) was not developed with
# government funds; (c) is a trade secret of 60East Technologies Inc.
# for all purposes of the Freedom of Information Act; and (d) is a
# commercial item and thus, pursuant to Section 12.212 of the Federal
# Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
# Government's use, duplication or disclosure of the computer software
# is subject to the restrictions set forth by 60East Technologies Inc..
##
###############################################################################
"""
module amps_sow

amps_sow provides easy access to AMPS SOW files. SOW files
are opened using the `open_file' function, and then accessed using the
returned Reader or Writer object. Other functions provide shortcuts to common
operations.

FUNCTIONS:
dump(file, limit) dumps the contents of a SOW file to stdout.
version(file) returns the version of a SOW file.
upgrade(old_file, new_file) upgrades from an older version of a SOW file to
 the latest.

open_file(path, mode, record_size, increment_size) opens a SOW file for reading or
 writing.

CLASSES:
Writer provides file writing.
Reader provides a records() generator that yields a sequence of maps or SowRecord objects representing the contents of the sow file.
SowRecord30 is yielded by Reader30's records() generator and represents a record from a version 3.0 or prior SOW
SowRecord40 is yielded by Reader40's records() generator and represents a record from a version 4.0 SOW
(note that SOW file versions do not correspond to AMPS server versions.)

"""

import amps_common
import array
import ctypes
import datetime
import glob
import gzip
import os
import struct
import sys
import time
from shutil import copy2
import math
from decimal import Decimal

#
# module-global vars
#
LATEST_VERSION = 6.0
# flags field can have the following values
SOW_INVALID_RECORD_FLAG = 1
SOW_HISTORICAL_RECORD_FLAG = 2
SOW_HISTORICAL_RECORD_DELETE_FLAG = 4
SOW_COMPRESSION_FLAG = 8
SOW_RECORD_FLAGS_MAX_VALUE_V2 = 32
SOW_RECORD_FLAGS_MAX_VALUE_V3 = 32

SOW_RECORD_IS_HISTORICAL_MASK = 2
SOW_RECORD_IS_HISTORICAL_DELETE_MASK = 4
SOW_RECORD_IS_COMPRESSED_MASK = 8
SOW_RECORD_IS_STRING_KEY_MASK = 16
SOW_RECORD_IS_TOPIC_MASK = 32
OMIT_DATA = False
DUMMY_DATA = '<removed from dump>'

#
# module-private global functions
#


def _amps_to_iso8601_timestamp(amps_time, is_localtime):
    """
    Converts amps journal time into an ISO8601 datetime string.
    """
    #it = amps_time / 1000000 - 210866803200  # subtract the unix epoch
    it = int(Decimal(amps_time)/Decimal(1000000.0) - 210866803200)
    ft = amps_time % 1000000
    if is_localtime:
        return time.strftime("%Y%m%dT%H%M%S", time.localtime(it)) + (".%.6d" % ft)
    else:
        return time.strftime("%Y%m%dT%H%M%S", time.gmtime(it)) + (".%.6d" % ft) + "Z"


def _posix_to_iso8601_timestamp(posix_time, is_localtime):
    """
    Converts posix seconds since unix epoch time into an ISO8601 datetime string.
    """
    class UTC(datetime.tzinfo):
        """
        private trivial UTC tzinfo implementation
        """

        def utcoffset(self, dt):
            return datetime.timedelta(0)

        def tzname(self, dt):
            return "Z"

        def dst(self, dt):
            return datetime.timedelta(0)
    if len(str(posix_time)) > 16:
        #
        # far larger a value than expected for posix time
        # posix_time is almost certainly an an amps timestamp
        #
        return _amps_to_iso8601_timestamp(posix_time, is_localtime)
    fmt_8601 = "%Y%m%dT%H%M%S"
    if is_localtime:
        local_tz = ""
        return datetime.datetime.fromtimestamp(posix_time).strftime(fmt_8601) + local_tz
    else:
        return datetime.datetime.fromtimestamp(posix_time, UTC()).strftime(fmt_8601 + "%Z")


def _extract_update_timestamp(rec, is_localtime):
    update_time_key = "update_time"
    if update_time_key in rec and rec[update_time_key]:
        return _amps_to_iso8601_timestamp(rec[update_time_key], is_localtime)
    return "0"


def _extract_expiration_str_pair(rec, is_localtime):
    exp_time_key = "expiration"
    exp_val = "0"
    exp_tag = "expiration"
    if exp_time_key in rec and rec[exp_time_key]:
        exp_val = _posix_to_iso8601_timestamp(rec[exp_time_key], is_localtime)
    fstrkey = "flagsStr"
    if fstrkey in rec and rec[fstrkey].find("historical_delete") > -1:
        exp_tag = "deleted"
    return (exp_tag, exp_val)


def _extract_topic_byte_count(rec):
    return rec["topic_len"]

#
# classes
#


class Reader10:

    def __init__(self, path, file_obj, header):
        self.SOW_VERSION = 1
        self.file_s = struct.Struct('21s1cQQ')
        self.version, self.nb, self.record_size, \
            self.number_of_records = self.file_s.unpack_from(header)
        self.sizing_chart = None

    def records(self):
        global OMIT_DATA
        global DUMMY_DATA
        record_s = struct.Struct('QQQQQQQ')
        valid_keys = 0
        multi_records = 0
        while(self.file):
            head = amps_common.read_exact(self.file, self.record_size)
            if(len(head) <= 0):
                break

            crc, flags, node_size, header_size, data_size, key, seq = record_s.unpack_from(head)
            crc = int(crc)

            # Pick-up the remaining records
            remainder_records = node_size / self.record_size - 1
            if(remainder_records > 0):
                multi_records = multi_records + 1
                head = head + amps_common.read_exact(self.file, self.record_size * remainder_records)

            if(not key == 0):
                valid_keys = valid_keys + 1
                self.total_data_size = self.total_data_size + data_size
                self.max_data_size = max(data_size, self.max_data_size)
                if(self.sizing_chart):
                    self.sizing_chart.add(record_s.size + header_size + data_size)
            header = self.head[record_s.size:record_s.size + header_size]
            data = DUMMY_DATA if OMIT_DATA else self.head[record_s.size +
                                                          header_size:record_s.size + header_size + self.data_size]
            allocated = (self.node_size / self.record_size + 1) * self.record_size
            yield {'crc': crc, 'flags': flags, 'node_size': node_size,
                   'header_size': header_size, 'data_size': data_size,
                   'key': key, 'seq': seq, 'header': header, 'data': data,
                   'allocated': allocated, 'expiration_time': 0, 'update_time': 0,
                   'correlation_id': '', 'correlation_id_len': 0,
                   'generation_count': 0}

    def get_metadata(self):
        self.file.seek(0)
        return amps_common.read_exact(self.file, 4096)

    def get_last_sync_txid(self):
        self.file_obj.seek(0)
        data = amps_common.read_exact(self.file_obj, 128)
        crc, sowkey, local_txid = struct.Struct('QQQ').unpack_from(data[64:])
        return local_txid


class Reader20:

    def __init__(self, path, file_obj, header):
        self.SOW_VERSION = 2
        self.file_s = struct.Struct('32sQQQ')
        self.version, self.record_size, self.number_of_records, \
            self.crc = self.file_s.unpack_from(header)
        self.version = self.version.decode().strip(chr(0))  # filter out the null bytes
        self.invalid_records = 0

    def records(self):
        global OMIT_DATA
        global DUMMY_DATA
        self.record_s = struct.Struct('IIIIIQQ')
        while(self.file):
            self.head = amps_common.read_exact(self.file, self.record_size)
            if(len(self.head) <= 0):
                break
            self.crc, self.valid, self.num_records, self.data_size, self.header_size, self.key, self.seq = self.record_s.unpack_from(
                self.head)
            # if( crc == 0 and num_records == 0 ):
            #   #import pdb; pdb.set_trace()
            #   invalid_records = invalid_records + 1
            #   continue

            if(not self.valid == 2779096485):
                #import pdb; pdb.set_trace()
                self.invalid_records = self.invalid_records + 1
                continue

            # Pick-up the remaining records
            if(self.num_records > 1):
                self.multi_records = self.multi_records + 1
                self.head = self.head + amps_common.read_exact(self.file, self.record_size * (self.num_records - 1))

            if(not self.key == 0):
                self.valid_keys = self.valid_keys + 1
                self.total_data_size = self.total_data_size + self.data_size
                self.max_data_size = max(self.data_size, self.max_data_size)
                if(self.sizing_chart):
                    self.sizing_chart.add(self.record_s.size + self.header_size + self.data_size)
            self.header = self.head[self.record_s.size:self.record_s.size + self.header_size]
            self.data = DUMMY_DATA if OMIT_DATA else self.head[self.record_s.size +
                                                               self.header_size:self.record_s.size + self.header_size + self.data_size]
            self.allocated = (self.num_records * self.record_size)
            yield {'crc': self.crc, 'valid': self.valid, 'num_records': self.num_records, 'data_size': self.data_size,
                   'header_size': self.header_size, 'key': self.key, 'seq': self.seq, 'allocated': self.allocated,
                   'expiration_time': 0, 'generation_count': 0, 'update_time': 0, 'flags': 0, 'header': self.header, 'data': self.data,
                   'correlation_id': '', 'correlation_id_len': 0}

    def get_metadata(self):
        self.file.seek(0)
        return amps_common.read_exact(self.file, 4096)

    def get_last_sync_txid(self):
        self.file_obj.seek(0)
        data = amps_common.read_exact(self.file_obj, 128)
        crc, sowkey, local_txid = struct.Struct('QQQ').unpack_from(data[64:])
        return local_txid


class Reader21:

    def __init__(self, path, file_obj, header):
        self.SOW_VERSION = 2.1
        self.file_s = struct.Struct('32sQQQ')
        self.version, self.record_size, self.number_of_records, \
            self.crc = self.file_s.unpack_from(header)
        self.version = self.version.decode().strip(chr(0))  # filter out the null bytes
        self.invalid_records = 0

    def records(self):
        global OMIT_DATA
        global DUMMY_DATA
        self.record_s = struct.Struct('IIIIIQQ')
        while(self.file):
            self.head = amps_common.read_exact(self.file, self.record_size)
            if(len(self.head) <= 0):
                break
            self.crc, self.valid, self.num_records, self.data_size, self.header_size, self.key, self.seq = self.record_s.unpack_from(
                self.head)
            if(self.crc == 0 and self.num_records == 0):
                #import pdb; pdb.set_trace()
                self.invalid_records = self.invalid_records + 1
                continue

            # Pick-up the remaining records
            if(self.num_records > 1):
                self.multi_records = self.multi_records + 1
                self.head = self.head + amps_common.read_exact(self.file, self.record_size * (self.num_records - 1))

            if(not self.key == 0):
                self.valid_keys = self.valid_keys + 1
                self.total_data_size = self.total_data_size + self.data_size
                self.max_data_size = max(self.data_size, self.max_data_size)
                if(self.sizing_chart):
                    self.sizing_chart.add(self.record_s.size + self.header_size + self.data_size)
            self.header = self.head[self.record_s.size:self.record_s.size + self.header_size]
            self.data = DUMMY_DATA if OMIT_DATA else self.head[self.record_s.size +
                                                               self.header_size:self.record_s.size + self.header_size + self.data_size]
            self.allocated = (self.num_records * self.record_size)

            yield {'crc': self.crc, 'valid': self.valid, 'num_records': self.num_records, 'data_size': self.data_size,
                   'header_size': self.header_size, 'key': self.key, 'seq': self.seq, 'allocated': self.allocated,
                   'expiration_time': 0, 'generation_count': 0, 'update_time': 0, 'flags': 0, 'header': self.header, 'data': self.data,
                   'correlation_id': '', 'correlation_id_len': 0}

    def get_metadata(self):
        self.file.seek(0)
        return amps_common.read_exact(self.file, 4096)

    def get_last_sync_txid(self):
        self.file_obj.seek(0)
        data = amps_common.read_exact(self.file_obj, 128)
        crc, sowkey, local_txid = struct.Struct('QQQ').unpack_from(data[64:])
        return local_txid


class SowRecord30(dict):
    def __init__(self, content):
        dict.__init__(self)
        self.update(content)

    def __str__(self):
        return self.as_string()

    def as_string(self, **kwargs):
        verbose = "verbose" in kwargs and kwargs["verbose"]
        if not verbose:
            return "%s" % self['data']
        else:
            return """key               = %(key)s
crc               = %(crc)s
valid             = %(valid)s
number of records = %(num_records)s
data size         = %(data_size)s
expiration        = %(expiration)s
seq               = %(seq)s
data              = [%(data)s]
   """ % self


class FileMetadata(dict):

    def __init__(self, file_name, version, record_size, compression_type, compression_options):
        dict.__init__(self)
        self['file_name'] = file_name
        self['version'] = version
        self['compression_type'] = compression_type
        self['compression_options'] = compression_options
        self['slab_details'] = []

        self['record_size'] = record_size
        self['max_data_size'] = 0
        self['multi_records'] = 0
        self['number_of_records'] = 0
        self['sizing_chart'] = None
        self['total_data_size'] = 0
        self['valid_keys'] = 0

    def as_string(self):
        s = """File                : %(file_name)20s
  Version             : %(version)20s
  Valid Keys          : %(valid_keys)20s
  Record Size         : %(record_size)20s
  Maximum Records     : %(number_of_records)20s
  Multirecords        : %(multi_records)20s
  """ % self
        if self['valid_keys'] > 0:
            s += "Maximum record size : %(max_data_size)20s\n" % self
            s += "Average record size : %20.2f\n" % (float(self['total_data_size']) / self['valid_keys'])
        else:
            s += "Maximum record size : %20s\n" % "(no data)"
            s += "Average record size : %20s\n" % "(no data)"
        if len(self['slab_details']) > 0:
            s += "Slab Count          : %20s\n" % len(self['slab_details'])
            for slab in self['slab_details']:
                s += """
    Slab Detail
      size          : %(size)20s
      file offset   : %(offset)20s
      valid count   : %(valid_count)20s
      invalid count : %(invalid_count)20s
      stored bytes  : %(stored_bytes)20s
      data bytes    : %(data_bytes)20s
      deleted bytes : %(deleted_bytes)20s
    """ % slab
        return s

    def _data_size(self, size):
        self['total_data_size'] += size
        self['max_data_size'] = max(size, self['max_data_size'])

    def _multi_record(self):
        self['multi_records'] += 1

    def _new_slab_detail(self, slab_size, file_offset):
        return {
           'size': slab_size,
           'offset': file_offset,
           'valid_count': 0,
           'invalid_count': 0,
           'stored_bytes': 0,
           'data_bytes': 0,
           'deleted_bytes': 0,
        }

    def _record(self, increment=1):
        self['number_of_records'] += increment

    def _sizing_chart(self, size):
        if self['sizing_chart']:
            self['sizing_chart'].add(size)

    def _slab_detail(self, detail):
        self['slab_details'].append(detail)

    def _valid_key(self):
        self['valid_keys'] += 1


class Reader30:

    def __init__(self, path, file_obj, header):
        self.file_obj = file_obj
        self.file_s = struct.Struct('32sQQQQ')
        self.version, self.record_size, \
            self.number_of_records, self.increment_size, \
            crc = struct.Struct('32sQQQQ').unpack_from(header)
        self.crc = int(crc)
        self.raw_version = self.version.decode().strip(chr(0))
        self.version = 3.0
        self.compression_type = ''
        self.compression_options = 0
        self.md = FileMetadata(path, str(self.raw_version), self.record_size,
                               self.compression_type, self.compression_options)
        self.metadata = self.md

    def records(self):
        global OMIT_DATA
        global DUMMY_DATA
        while True:
            record_s = struct.Struct('IIIIQQQ')
            crc = 0
            num_records = 0
            while crc == 0 and num_records == 0:
                head = amps_common.read_exact(self.file_obj, self.record_size)
                if len(head) <= 0:
                    return
                crc, valid, num_records, data_size, expiration, key, seq = record_s.unpack_from(head)

            # Pick-up the remaining records
            if num_records > 1:
                self.md._multi_record()
                head = head + amps_common.read_exact(self.file_obj, self.record_size * (num_records - 1))

            if not key == 0:
                self.md._valid_key()
                self.md._data_size(data_size)
                self.md._sizing_chart(record_s.size + data_size)

            data = DUMMY_DATA if OMIT_DATA else head[record_s.size:record_s.size + data_size]
            try:
                data = data.decode('utf-8', 'ignore')
            except UnicodeDecodeError:
                pass
            self.md._record()
            yield SowRecord30({
               'allocated': int(num_records * self.record_size),
               'crc': crc,
               'data': data,
               'data_size': data_size,
               'expiration': expiration,
               'expiration_time': expiration,
               'key': key,
               'num_records': num_records,
               'seq': seq,
               'valid': valid,
               'update_time': 0,
               'generation_count': 0,
               'flags': 0,
               'flagsStr': '0',
               'correlation_id': '',
               'correlation_id_len': 0
            })

    def get_metadata(self):
        self.file.seek(0)
        return amps_common.read_exact(self.file, 4096)

    def get_last_sync_txid(self):
        self.file_obj.seek(0)
        data = amps_common.read_exact(self.file_obj, 128)
        crc, sowkey, local_txid = struct.Struct('QQQ').unpack_from(data[64:])
        return local_txid


def _timestamp_to_datetime(ts):
    if ts is None:
        return None
    elif len(ts) > 15:
        ts = ts[:15]
    try:
        fmt = "%Y%m%dT%H%M%S"
        return datetime.datetime.strptime(ts, fmt)
    except Exception as e:
        # print "exception: %s" % e
        return None


def _timestamp_in_the_past(ts):
    dt = _timestamp_to_datetime(ts)
    if dt is None:
        return False
    return datetime.datetime.utcnow() > dt


class SowRecord40(dict):

    def __init__(self, content):
        dict.__init__(self)
        self.update(content)

    def __str__(self):
        return self.as_string()

    def as_string(self, **kwargs):
        verbose = "verbose" in kwargs and kwargs["verbose"]
        is_localtime = "is_localtime" in kwargs and kwargs["is_localtime"]
        include_expired = "include_expired" in kwargs and kwargs["include_expired"]
        include_read_offset = "include_read_offset" in kwargs and kwargs["include_read_offset"]
        iso_time = _extract_update_timestamp(self, is_localtime)
        exp_pair = _extract_expiration_str_pair(self, is_localtime)
        is_expired = _timestamp_in_the_past(exp_pair[1])
        fmtstr = "%(data)s"
        if verbose:
            fmtstr = """key               = %(key)s
crc               = %(crc)s
flags             = %(flagsStr)s"""
            if include_read_offset:
                fmtstr += """
read offset       = %(read_offset)s"""
            fmtstr += """
slab offset       = %(slab_offset)s
allocated         = %(allocated)s
data size         = %(data_size)s"""
            if len(exp_pair) > 0:
                fmtstr += "\n%-16s  = %s" % exp_pair
            fmtstr += """
iso8601 timestamp = """ + iso_time + """
local txid        = %(seq)s
string key        = [%(string_key)s]
correlation id    = [%(correlation_id)s]
data              = [%(data)s]
   """
        if not is_expired or include_expired:
            return fmtstr % self


class SowRecord50(dict):

    def __init__(self, content):
        dict.__init__(self)
        self.update(content)

    def __str__(self):
        return self.as_string()

    def as_string(self, **kwargs):
        verbose = "verbose" in kwargs and kwargs["verbose"]
        is_localtime = "is_localtime" in kwargs and kwargs["is_localtime"]
        include_expired = "include_expired" in kwargs and kwargs["include_expired"]
        include_read_offset = "include_read_offset" in kwargs and kwargs["include_read_offset"]
        iso_time = _extract_update_timestamp(self, is_localtime)
        exp_pair = _extract_expiration_str_pair(self, is_localtime)
        is_expired = _timestamp_in_the_past(exp_pair[1])
        topic_byte_count = self["topic_len"]
        fmtstr = "%(data)s"
        if verbose:
            fmtstr = """key               = %(key)s
crc               = %(crc)s
flags             = %(flagsStr)s"""
            if include_read_offset:
                fmtstr += """
read offset       = %(read_offset)s"""
            fmtstr += """
slab offset       = %(slab_offset)s
allocated         = %(allocated)s
data size         = %(data_size)s"""
            if len(exp_pair) > 0:
                fmtstr += "\n%-16s  = %s" % exp_pair
            fmtstr += """
iso8601 timestamp = """ + iso_time + """
local txid        = %(seq)s
string key        = [%(string_key)s]
correlation id    = [%(correlation_id)s]"""
            if topic_byte_count:
                fmtstr += """
topic             = [%(topic)s]"""
            fmtstr += """
data              = [%(data)s]
   """
        if not is_expired or include_expired:
            return fmtstr % self
        else:
            return None


class Reader40:

    def __init__(self, path, file_obj, header, validate=True):
        self.file_obj = file_obj
        self.version, self.size, self.record_size, \
            self.increment_size, self.meta_generation_count, \
            self.crc = struct.Struct('16sQQQQI').unpack_from(header)
        self.raw_version = self.version.decode().strip(chr(0))
        self.version = 4.0
        self.string_key = ''
        self.string_key_len = 0
        self.compression_type = ''
        self.compression_options = 0
        self.md = FileMetadata(path, str(self.raw_version), self.record_size,
                               self.compression_type, self.compression_options)
        self.metadata = self.md
        self.invalid_records = 0
        self.crc_generator = None
        if validate:
            self.crc_generator = amps_common.Crc32()
            calc_crc = self.crc_generator.crc32(header, 0, end=48) & 0xffffffff
            if calc_crc != self.crc:
                raise Exception("bad file header. found crc %s but should be %s" % (self.crc, calc_crc))

    def records(self):
        global OMIT_DATA
        global DUMMY_DATA
        record_s = struct.Struct('IIIIQQQQQQ')
        read_size = 128
        offset = 4096  # metadata is first 4096 bytes
        while self.file_obj:
            magic = '00000000'
            slab_size = int(0xffffffffffffffff)
            file_offset = int(0xffffffffffffffff)
            crc = int(0xffffffff)
            self.file_obj.seek(offset)
            head = amps_common.read_exact(self.file_obj, read_size)
            if len(head) <= 0:
                break

            magic, slab_size, \
                file_offset, crc = struct.Struct('8sQQI').unpack_from(head)

            slab_detail = self.md._new_slab_detail(slab_size, file_offset)

            if magic.find(b'AMPSSLAB') != 0:
                # if can't find magic after the first offset then search is done
                if offset > 4096:
                    break
                continue
            if self.crc_generator is not None:
                calc_crc = self.crc_generator.crc32(head, 0, end=24) & 0xffffffff
                if calc_crc != crc:
                    print(("bad slab header. found crc %s but should be %s" % (crc, calc_crc)))
                    break
            offset = file_offset + slab_size
            get = 0

            while get < slab_size:
                head = amps_common.read_exact(self.file_obj, read_size)
                if len(head) <= 0:
                    break
                crc, flags, data_size, allocated, slab_offset, expiration_time, \
                    update_time, generation_count, \
                    key, seq = record_s.unpack_from(head)
                if flags > SOW_RECORD_FLAGS_MAX_VALUE_V2 or data_size > allocated or crc == 0:
                    # might be more efficient to seek the file and move to next slab
                    get = get + read_size
                    continue

                flags_array = []
                if flags & SOW_RECORD_IS_HISTORICAL_MASK:
                    flags_array.append('historical')
                if flags & SOW_RECORD_IS_HISTORICAL_DELETE_MASK:
                    flags_array.append('historical_delete')
                if flags & SOW_RECORD_IS_COMPRESSED_MASK:
                    flags_array.append('compressed')
                if flags & SOW_RECORD_IS_STRING_KEY_MASK:
                    flags_array.append('string_key')

                flagsStr = ''.join(flags_array)

                read_offset = get
                slab_remaining = slab_size - get
                readable = min(slab_remaining, allocated - read_size)
                get = get + allocated
                if data_size == 0 and flags & SOW_INVALID_RECORD_FLAG:
                    amps_common.read_exact(self.file_obj, readable)
                    self.invalid_records += 1
                    slab_detail['invalid_count'] += 1
                    slab_detail['deleted_bytes'] += allocated
                    continue
                # Pick-up the remaining records
                if allocated > read_size:
                    head = head + amps_common.read_exact(self.file_obj, readable)
                if allocated > self.record_size:
                    self.md._record(allocated / self.record_size)
                    self.md._multi_record()
                else:
                    self.md._record()

                if self.crc_generator is not None:
                    recordCrc = int(~0) & 0xffffffff
                    crc_length = 64 + data_size
                    recordCrc = self.crc_generator.crc32(head, recordCrc, offset=4, end=crc_length) & 0xffffffff
                    if recordCrc != crc:
                        self.invalid_records += 1
                        slab_detail['invalid_count'] += 1
                        slab_detail['deleted_bytes'] += allocated
                        continue
                if key != 0:
                    self.md._valid_key()
                    slab_detail['valid_count'] += 1
                    slab_detail['stored_bytes'] += allocated
                    slab_detail['data_bytes'] += data_size
                    self.md._data_size(data_size)
                    self.md._sizing_chart(record_s.size + data_size)
                data = DUMMY_DATA if OMIT_DATA else head[record_s.size:record_s.size + data_size]
                try:
                    data = data.decode('utf-8', 'ignore')
                except UnicodeDecodeError:
                    pass
                yield SowRecord40({
                   'crc': crc,
                   'data': data,
                   'data_size': data_size,
                   'expiration': expiration_time,
                   'key': key,
                   'seq': seq,
                   'flags': flags,
                   'flagsStr': flagsStr,
                   'allocated': allocated,
                   'expiration_time': expiration_time,
                   'update_time': update_time,
                   'generation_count': generation_count,
                   'slab_offset': slab_offset,
                   'read_offset': read_offset,
                   'string_key': '',
                   'string_key_len': 0,
                   'correlation_id': '',
                   'correlation_id_len': 0
                })

            if (slab_size > 0):
                self.md._slab_detail(slab_detail)

    def get_metadata(self):
        self.file_obj.seek(0)
        return amps_common.read_exact(self.file_obj, 4096)

    def get_last_sync_txid(self):
        self.file_obj.seek(0)
        data = amps_common.read_exact(self.file_obj, 128)
        crc, sowkey, local_txid = struct.Struct('QQQ').unpack_from(data[64:])
        return local_txid


class Reader50:
    def __init__(self, path, file_obj, header, validate=True):
        self.file_obj = file_obj
        self.version, self.size, self.record_size, \
            self.increment_size, self.meta_generation_count, \
            self.crc = struct.Struct('16sQQQQI').unpack_from(header)
        self.raw_version = self.version.decode().strip(chr(0))
        self.version = 5.0
        self.max_flags_valid_value = 63
        self.compression_type = ''
        self.compression_options = 0
        self.md = FileMetadata(path, str(self.raw_version), self.record_size,
                               self.compression_type, self.compression_options)
        self.metadata = self.md
        self.invalid_records = 0
        self.crc_generator = None
        if validate:
            self.crc_generator = amps_common.Crc32()
            calc_crc = self.crc_generator.crc32(header, 0, end=48) & 0xffffffff
            if calc_crc != self.crc:
                raise Exception("bad file header. found crc %s but should be %s" % (self.crc, calc_crc))

    def records(self, binary=False):
        global OMIT_DATA
        global DUMMY_DATA
        record_s = struct.Struct('IIIIQQQIIQQ')
        topic_s = struct.Struct('I')
        read_size = 128
        offset = 4096  # metadata is first 4096 bytes
        while self.file_obj:
            magic = '00000000'
            slab_size = int(0xffffffffffffffff)
            file_offset = int(0xffffffffffffffff)
            crc = int(0xffffffff)
            self.file_obj.seek(offset)
            head = amps_common.read_exact(self.file_obj, read_size)
            if len(head) <= 0:
                break

            magic, slab_size, \
                file_offset, crc = struct.Struct('8sQQI').unpack_from(head)

            slab_detail = self.md._new_slab_detail(slab_size, file_offset)

            if magic.find(b'AMPSSLAB') != 0:
                # if can't find magic after the first offset then search is done
                if offset > 4096:
                    break
                continue
            if self.crc_generator is not None:
                calc_crc = 0
                calc_crc = self.crc_generator.crc32(head[:24], calc_crc) & 0xffffffff
                if calc_crc != crc:
                    print(("bad slab header. found crc %s but should be %s" % (crc, calc_crc)))
                    break
            offset = file_offset + slab_size
            get = 128
            while get < slab_size:
                self.file_obj.seek(file_offset + get)
                head = amps_common.read_exact(self.file_obj, read_size)

                if len(head) <= 0:
                    break

                crc, flags, data_size, allocated, slab_offset, expiration_time, \
                    update_time, correlation_id_len, string_key_len, \
                    key, txid = record_s.unpack_from(head)

                if flags > self.max_flags_valid_value or data_size > allocated or crc == 0:
                    # might be more efficient to seek the file and move to next slab
                    print(("CHECK 1 GET = %d FLAGS = %d DATA SIZE = %d ALLOCATED = %lu KEY = %d TXID = %d REMAINING = %lu"
                           % (get, flags, data_size, allocated, key, txid, slab_size - get)))
                    get += read_size
                    continue

                read_offset = get
                slab_remaining = slab_size - get
                readable = min(slab_remaining, allocated - read_size)

                if allocated > read_size:
                    head = head + amps_common.read_exact(self.file_obj, readable)

                topic_byte_count = 0
                topic_crc_length = 0
                topic = None
                if flags & SOW_RECORD_IS_TOPIC_MASK:
                    topic_offset = record_s.size + string_key_len + correlation_id_len + data_size
                    data_offset = record_s.size + string_key_len + correlation_id_len
                    (topic_byte_count,) = topic_s.unpack_from(head[topic_offset:topic_offset + 4])
                    topic = (head[topic_offset + 4:topic_offset + 4 + topic_byte_count]).decode('latin-1')
                    topic_crc_length = 4 + topic_byte_count

                if self.crc_generator is not None:
                    recordCrc = int(~0) & 0xffffffff
                    crc_length = 64 + string_key_len + correlation_id_len + data_size + topic_crc_length
                    recordCrc = self.crc_generator.crc32(head, recordCrc, offset=4, end=crc_length) & 0xffffffff
                    if recordCrc != crc:
                        self.invalid_records += 1
                        remaining = slab_size - get
                        # print("CRC ERROR GET = %d FLAGS = %d DATA SIZE = %d ALLOCATED = %d KEY = %d TXID = %d SLAB OFFSET = %d SLAB SIZE = %d REMAINING = %d"\
                        #      % (get,flags,data_size,allocated,key,txid,slab_offset,slab_size,remaining))
                        slab_detail['invalid_count'] += 1
                        slab_detail['deleted_bytes'] += allocated
                        get += read_size
                        continue

                last_size = allocated
                flags_array = []
                if flags & SOW_RECORD_IS_HISTORICAL_MASK:
                    flags_array.append('historical')
                if flags & SOW_RECORD_IS_HISTORICAL_DELETE_MASK:
                    flags_array.append('historical_delete')
                if flags & SOW_RECORD_IS_COMPRESSED_MASK:
                    flags_array.append('compressed')
                if flags & SOW_RECORD_IS_STRING_KEY_MASK:
                    flags_array.append('string_key')
                if flags & SOW_RECORD_IS_TOPIC_MASK:
                    flags_array.append('topic')

                flagsStr = ','.join(flags_array)

                if data_size == 0 and flags & SOW_INVALID_RECORD_FLAG:
                    # print("INVALID GET = %d FLAGS = %d KEY = %lu TXID = %lu ALLOCATED = %lu REMAINING = %lu"\
                    #      % (get,flags,key,txid,allocated,slab_size - get))
                    self.invalid_records += 1
                    slab_detail['invalid_count'] += 1
                    slab_detail['deleted_bytes'] += allocated
                    get += allocated
                    continue

                if allocated > self.record_size:
                    self.md._record(allocated / self.record_size)
                    self.md._multi_record()
                else:
                    self.md._record()

                get += allocated

                if key != 0:
                    self.md._valid_key()
                    slab_detail['valid_count'] += 1
                    slab_detail['stored_bytes'] += allocated
                    slab_detail['data_bytes'] += data_size
                    self.md._data_size(data_size)
                    self.md._sizing_chart(record_s.size + data_size)
                string_key = head[record_s.size:record_s.size + string_key_len]
                correlation_id = head[record_s.size + string_key_len:record_s.size +
                                      string_key_len + correlation_id_len]
                data = DUMMY_DATA if OMIT_DATA else head[record_s.size + string_key_len +
                                                         correlation_id_len:record_s.size + string_key_len + correlation_id_len + data_size]
                # Preserve metadata even if we're redacting the actual message data
                data_size = len(head[record_s.size + string_key_len +
                                     correlation_id_len:record_s.size + string_key_len + correlation_id_len + data_size])
                if not binary:
                    try:
                        if hasattr(data, 'decode'):
                            data = data.decode('utf-8', 'ignore')
                        if not OMIT_DATA:
                            data_size = len(data)
                    except UnicodeDecodeError:
                        pass
                yield SowRecord50({
                   'crc': crc,
                   'data': data,
                   'data_size': data_size,
                   'expiration': expiration_time,
                   'correlation_id': correlation_id.decode(),
                   'correlation_id_len': correlation_id_len,
                   'string_key': string_key.decode(),
                   'string_key_len': string_key_len,
                   'topic_len': topic_byte_count,
                   'key': key,
                   'topic': topic,
                   'seq': txid,
                   'flags': flags,
                   'flagsStr': flagsStr,
                   'allocated': allocated,
                   'expiration_time': expiration_time,
                   'update_time': update_time,
                   'generation_count': 0,
                   'slab_offset': slab_offset,
                   'read_offset': read_offset,
                })

            if (slab_size > 0):
                self.md._slab_detail(slab_detail)

    def get_metadata(self):
        self.file_obj.seek(0)
        return amps_common.read_exact(self.file_obj, 4096)

    def get_last_sync_txid(self):
        self.file_obj.seek(0)
        data = amps_common.read_exact(self.file_obj, 192)
        crc, sowkey, local_txid = struct.Struct('QQQ').unpack_from(data[128:])
        return local_txid


class Reader60(Reader50):

    def __init__(self, path, file_obj, header, validate=True):
        self.file_obj = file_obj
        self.version, self.compression_type, self.compression_options, self.size, self.record_size, \
            self.increment_size, self.meta_generation_count, \
            self.crc = struct.Struct('16s8sQQQQQI').unpack_from(header)
        # record_size has been co-opted for 'valid-key-count'
        # and thus use 128 as the record_size given that the
        # latest version has is hard coded.
        self.record_size = 128
        self.raw_version = self.version.decode().strip(chr(0))
        self.raw_compression_type = str(self.compression_type).strip(chr(0))
        self.version = 6.0
        self.max_flags_valid_value = 63
        self.md = FileMetadata(path, str(self.raw_version), self.record_size,
                               str(self.raw_compression_type), self.compression_options)
        self.metadata = self.md
        self.invalid_records = 0
        self.crc_generator = None
        if validate:
            self.crc_generator = amps_common.Crc32()
            calc_crc = self.crc_generator.crc32(header, 0, end=64) & 0xffffffff
            if calc_crc != self.crc:
                raise Exception("bad file header. found crc %s but should be %s" % (self.crc, calc_crc))


LATEST_VERSION_STRING = 'amps-sow-v3.0'
COMPRESSION_TYPE = 'gz'
WRITER_VERSION_STRING = LATEST_VERSION_STRING.ljust(16, '\0')
WRITER_COMPRESSION_TYPE = COMPRESSION_TYPE.ljust(8, '\0')
WRITER_COMPRESSION_OPTIONS = 9


class Writer:
    # initializes the file
    def __init__(self, filename, record_size, allocation_size, compression_type, compression_options):
        self.PAGE_SIZE = 4096
        self.PAGE_SIZE_MASK = self.PAGE_SIZE - 1
        self.ALIGN_SIZE = 128
        self.ALIGN_SIZE_MASK = self.ALIGN_SIZE - 1
        self.RECORD_HEADER_SIZE = 64
        self.SLAB_LABEL_SIZE = 128
        self.DEFAULT_PADDING = 16

        self._filename = filename
        self.crc_generator = amps_common.Crc32()
        self._allocation_size = allocation_size
        self._slab_max_size = allocation_size
        self._record_size = record_size
        self._num_records = 0
        self._compression_type = compression_type
        self._compression_options = compression_options
        # metadata is first self.PAGE_SIZE bytes
        self._file_size = 0
        self._increment_size = allocation_size
        self._meta_generation_count = 0
        self._read_size = 128
        self._slab_count = 0
        self._slab_written = 0
        self._label = amps_common.FileLabel("AMPSSLAB", 0, self.PAGE_SIZE, 0)
        self._label_struct = struct.Struct('8sQQI')
        # the canonical field order and format here is defined in amps_sow_data_store_header.hpp
        self._format = 'IIIIQQQIIQQ{0}s{1}s{2}s'
        #self._slab_max_size = 12812288
        #self._slab_max_size = 1282048
        # write header meetadata for file, fill first slab area
        # fill first slab header area
        self._file = open(self._filename, 'wb')
        self._file.seek(0)
        self._fill_gap(self.PAGE_SIZE)
        self._write_header()
        self._file.seek(self._label.offset)
        self._fill_gap(self.SLAB_LABEL_SIZE)
        self._slab_written = self.SLAB_LABEL_SIZE
        self._file_size = self._file_size + self.SLAB_LABEL_SIZE

    def _compute_allocated(self, data_size):
        min_size = self.RECORD_HEADER_SIZE + data_size + self.DEFAULT_PADDING
        return (min_size + self.ALIGN_SIZE_MASK) & ~self.ALIGN_SIZE_MASK

    # computes a CRC for a hunk of data and returns it formatted as a set of
    # bytes suitable for writing.
    def _packCRC(self, data):
        return struct.pack('I', int(self.crc_generator.crc32(data, 0) & 0xffffffff))

    def write_metadata(self, data):
        self._file.seek(0)
        self._file.write(data)

    def write_last_sync_txid(self, txid):
        self._file.seek(128)
        s = struct.Struct('QQQ')
        header = s.pack(0, 0, txid)
        self._file.write(header)

    def _write_header(self):
        self._file.seek(0)
        s = struct.Struct('16s8sQQQQQ')
        header = s.pack(WRITER_VERSION_STRING, self._compression_type, self._compression_options,
                        self._file_size, self._record_size, self._increment_size, self._meta_generation_count)
        self._file.write(header)
        self._file.write(self._packCRC(header))

    def _close_slab(self):
        if(self._slab_written < self._slab_max_size):
            self._file.seek(self._label.offset + self._slab_written)
            bytes_to_write = self._slab_max_size - self._slab_written
            self._fill_gap(bytes_to_write)
            self._slab_written = self._slab_written + bytes_to_write
            self._file_size = self._file_size + bytes_to_write
        else:
            # Min is a page size, so only check when more than min
            if(self._slab_written % self.PAGE_SIZE != 0):
                self._file.seek(self._label.offset + self._slab_written)
                bytes_to_write = self.PAGE_SIZE - (self._slab_written % self.PAGE_SIZE)
                self._fill_gap(bytes_to_write)
                self._slab_written = self._slab_written + bytes_to_write
                self._file_size = self._file_size + bytes_to_write
        self._file.seek(self._label.offset)
        self._label.size = self._slab_max_size
        self._label.crc = int(0)
        str = self._label_struct.pack(self._label.magic, self._label.size, self._label.offset, self._label.crc)
        self._label.crc = self.crc_generator.crc32(str[:-4], self._label.crc) & 0xffffffff
        str = self._label_struct.pack(self._label.magic, self._label.size, self._label.offset, self._label.crc)
        self._file.write(str)
        self._slab_count = self._slab_count + 1
        self._label.offset = self._file_size + self.PAGE_SIZE
        self._label.size = 0

    def close(self):
        self._close_slab()
        self._write_header()
        self._file.close()

    def write(self, record):
        if(self._slab_written == 0):
            # new slab, fill header area
            self._file.seek(self._label.offset)
            self._fill_gap(self._record_size)
            self._slab_written = self._record_size
            self._file_size = self._file_size + self._record_size

        allocated = self._compute_allocated(record['data_size'])

        if(self._slab_written + allocated > self._slab_max_size):
            # Time for a new slab - first close out the previous slab
            self._close_slab()
            self._file.seek(self._label.offset)
            self._fill_gap(self.SLAB_LABEL_SIZE)
            self._slab_written = self.SLAB_LABEL_SIZE
            self._file_size = self._file_size + self.SLAB_LABEL_SIZE
        self._file.seek(self._label.offset + self._slab_written)
        format = self._format.format(record['string_key_len'], record['correlation_id_len'], record['data_size'])
        # the canonical field order and format here is defined in amps_sow_data_store_header.hpp
        recordBytes = struct.pack(format,
                                  record['crc'],
                                  record['flags'],
                                  record['data_size'],
                                  allocated,
                                  self._label.offset,
                                  record['expiration_time'],
                                  record['update_time'],
                                  record['correlation_id_len'],
                                  record['string_key_len'],
                                  record['key'],
                                  record['seq'],
                                  str(record['string_key']),
                                  str(record['correlation_id']),
                                  str(record['data']))

        recordCrc = int(~0) & 0xffffffff
        recordCrc = self.crc_generator.crc32(recordBytes, recordCrc, offset=4) & 0xffffffff
        # the canonical field order and format here is defined in amps_sow_data_store_header.hpp
        recordBytes = struct.pack(format,
                                  recordCrc,
                                  record['flags'],
                                  record['data_size'],
                                  allocated,
                                  self._label.offset,
                                  record['expiration_time'],
                                  record['update_time'],
                                  record['correlation_id_len'],
                                  record['string_key_len'],
                                  record['key'],
                                  record['seq'],
                                  str(record['string_key']),
                                  str(record['correlation_id']),
                                  str(record['data']))
        self._file.write(recordBytes)
        bytes_to_write = allocated - len(recordBytes)
        self._fill_gap(bytes_to_write)
        self._slab_written = self._slab_written + allocated
        self._file_size = self._file_size + allocated

    # fills out the file, at the current position, with a bunch of \0s
    def _fill_gap(self, bytes_to_write):
        self._file.write('\0'.ljust(bytes_to_write, '\0'))


def open_file(path, mode='r', record_size=None, increment_size=None, validate=True):
    """Opens a sow file at the given path and returns a sow file object.
    :param path: the full path to the given sow file.
    :param mode: `r' to read the file, `w' to write, defaults to 'r'.
    :param record_size: the record size for this sow file. Required when writing.
    :type record_size: int
    :param increment_size: the increment size for this sow file.  Required when writing.
    :type  increment_size: int
    :param validate: validate each record's crc,
    :returns: a :class:Reader10 instance or :class:Reader20 :class:Reader21 instance or :class:Reader40 instance or :class:Writer instance, depending on the chosen `mode'.
    """
    valid_modes = ['r', 'w']
    if mode is None or not mode in valid_modes:
        raise RuntimeError("unknown mode: '%s'" % mode)
    elif mode == 'w' and (record_size is None or increment_size is None):
        raise RuntimeError("must supply record_size and increment_size when writing sow file.")
    elif mode == 'r':
        return open_reader(path, validate=validate)
    elif mode == 'w':
        return open_writer(path, record_size, increment_size)


#
# module-public global functions
#
def get_data(sow_file):
    data = []
    reader = open_file(sow_file)
    for record in reader.records():
        data.append(record['data'])
    return data


def open_reader(path, validate=True, omit_data=False):
    global OMIT_DATA
    OMIT_DATA = omit_data

    if hasattr(path, 'decode'):
        path = path.decode()
    try:
        if path.endswith(".gz"):
            file_obj = gzip.open(path, 'rb')
        else:
            file_obj = open(path, 'rb')
    except IOError as e:
        sys.exit("Error: %s\n" % e)
    header = amps_common.read_exact(file_obj, 4096)
    if header.find(b'amps::amps_pstore/1.0') == 0:
        return Reader10(path, file_obj, header)
    elif header.find(b'persist::gpstore/1.0') == 0:
        return Reader20(path, file_obj, header)
    elif header.find(b'persist::gpstore/1.1') == 0:
        return Reader21(path, file_obj, header)
    elif header.find(b'persist::gpstore/3.0') == 0:
        return Reader30(path, file_obj, header)
    elif header.find(b'amps-sow-v1.0') == 0:
        return Reader40(path, file_obj, header, validate=validate)
    elif header.find(b'amps-sow-v2.0') == 0:
        return Reader50(path, file_obj, header, validate=validate)
    elif header.find(b'amps-sow-v3.0') == 0:
        return Reader60(path, file_obj, header, validate=validate)
    else:
        file_obj.close()
        sys.exit("Error: This file is not in a recognized format, " +
                        "version string reported: %s\n" %
                        header[0:header.find(b'0')][:20])


def _align_to_page_size(value):
    PAGE_SIZE = 4096
    PAGE_SIZE_MASK = PAGE_SIZE - 1
    return (value + PAGE_SIZE_MASK) & ~PAGE_SIZE_MASK


def _compute_extend_size(value):
    WRITE_SIZE = 16 * 1024
    WRITE_SIZE_MASK = WRITE_SIZE - 1
    return (value + WRITE_SIZE_MASK) & ~WRITE_SIZE_MASK


def open_writer(path, record_size, increment_size):
    MIN_INCREMENT_PAGE_COUNT = 256
    MIN_STORE_SIZe = 128
    SLAB_LABEL_SIZE = 128
    ALIGN_SIZE = 128
    ALIGN_SIZE_MASK = ALIGN_SIZE - 1
    PAGE_SIZE = 4096
    PAGE_SIZE_MASK = PAGE_SIZE - 1
    WRITE_SIZE = 16 * 1024
    WRITE_SIZE_MASK = WRITE_SIZE - 1

    record_size = ALIGN_SIZE

    if increment_size < 128:
        increment_size = 128
    elif increment_size > 1000000:
        increment_size = 1000000

    recordSize = (record_size + ALIGN_SIZE_MASK) & ~ALIGN_SIZE_MASK
    incrementSize = (recordSize * increment_size) + SLAB_LABEL_SIZE
    alignedSize = _align_to_page_size(incrementSize)
    extendSize = _align_to_page_size(_compute_extend_size(alignedSize))
    minSize = MIN_INCREMENT_PAGE_COUNT * PAGE_SIZE
    allocationSize = max(extendSize, minSize)
    compressionType = WRITER_COMPRESSION_TYPE
    compressionOptions = WRITER_COMPRESSION_OPTIONS

    return Writer(path, recordSize, allocationSize, compressionType, compressionOptions)


def upgrade(old_file, new_file, record_size=512, increment_size=10000):
    record_size = int(record_size)
    increment_size = int(increment_size)
    old_file_name = old_file

    old_file = open_file(old_file)
    last_sync_txid = old_file.get_last_sync_txid()
    if version(old_file_name) == LATEST_VERSION:
        copy2(old_file_name, new_file)
        return

    new_file = open_file(new_file, 'w', record_size, increment_size)

    last_txid = 0
    if last_sync_txid:
        for aRecord in old_file.records():
            new_file.write(aRecord)

    else:
        for aRecord in old_file.records():
            new_file.write(aRecord)
            txid = aRecord['seq']
            if txid > last_txid:
                last_txid = txid

    if last_sync_txid == last_txid:
        metadata = old_file.get_metadata()
        new_file.write_metadata(metadata)
    elif last_sync_txid:
        new_file.write_last_sync_txid(last_sync_txid)
    else:
        new_file.write_last_sync_txid(last_txid)

    new_file.close()


def version(sow_file):
    try:
        if hasattr(sow_file, 'decode'):
            sow_file = sow_file.decode()
        if sow_file.split('.')[-1] == 'gz':
            file = gzip.open(sow_file, 'rb')
        else:
            file = open(sow_file, 'rb')
    except IOError:
        sys.stderr.write("Error: File '%s' not found.\n" % sow_file)
        sys.exit(1)
    sow_meta_data = amps_common.read_exact(file, 4096)
    if sow_meta_data.find(b'amps::amps_pstore/1.0') == 0:
        return 1
    elif sow_meta_data.find(b'persist::gpstore/1.0') == 0:
        return 2
    elif sow_meta_data.find(b'persist::gpstore/1.1') == 0:
        return 2.1
    elif sow_meta_data.find(b'persist::gpstore/3.0') == 0:
        return 3.0
    elif sow_meta_data.find(b'amps-sow-v1.0') == 0:
        return 4.0
    elif sow_meta_data.find(b'amps-sow-v2.0') == 0:
        return 5.0
    elif sow_meta_data.find(b'amps-sow-v3.0') == 0:
        return 6.0
    else:
        return -1


def is_up_to_date(file, print_debug_info_to=None):
    if print_debug_info_to is not None:
        print_debug_info_to.write("version(%s)=%s\n" % (file, version(file)))
        print_debug_info_to.write("LATEST_VERSION=%s\n" % LATEST_VERSION)
        print_debug_info_to.write("is_up_to_date(%s) returning %s\n" % (file, version(file)))
        print_debug_info_to.flush()
    return version(file) == LATEST_VERSION

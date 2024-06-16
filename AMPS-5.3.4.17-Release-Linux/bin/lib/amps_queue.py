###############################################################################
##
## Copyright (c) 2012-2017 60East Technologies Inc., All Rights Reserved.
##
## This computer software is owned by 60East Technologies Inc. and is
## protected by U.S. copyright laws and other laws and by international
## treaties.  This computer software is furnished by 60East Technologies
## Inc. pursuant to a written license agreement and may be used, copied,
## transmitted, and stored only in accordance with the terms of such
## license agreement and with the inclusion of the above copyright notice.
## This computer software or any other copies thereof may not be provided
## or otherwise made available to any other person.
##
## U.S. Government Restricted Rights.  This computer software: (a) was
## developed at private expense and is in all respects the proprietary
## information of 60East Technologies Inc.; (b) was not developed with
## government funds; (c) is a trade secret of 60East Technologies Inc.
## for all purposes of the Freedom of Information Act; and (d) is a
## commercial item and thus, pursuant to Section 12.212 of the Federal
## Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
## Government's use, duplication or disclosure of the computer software
## is subject to the restrictions set forth by 60East Technologies Inc..
##
###############################################################################

"""
module amps_queue

amps_ack provides easy access to AMPS queues.ack files. queues.ack files
are opened using the `open_file' function, and then accessed using the
returned file-like object. Other functions provide shortcuts to common
operations.

FUNCTIONS:
dump(file, limit) dumps the contents of a queues.ack file to stdout.
version(file) returns the version of a queues.ack file.
open_file(path, mode, record_size) opens a queues.ack file for reading or
 writing.

CLASSES:
File is the class for queues.ack file reading.
Writer provides file writing.
Record40 represents a single record from a store version 4.0 queues.ack.

"""



import glob
import gzip
import struct
import sys
import amps_common
import os
import time
from shutil import copy2
import math
from decimal import Decimal

LATEST_VERSION=4.0
UPGRADE=False
IS_LOCALTIME = False

def dump(file, limit=None):
   """Function for dumping the contents of a queues.ack file to stdout.

   :param file: the path to the queues.ack file to dump.
   :type file: str
   :param limit: the number maximum number of records to print from the file.
   :type limit: int
   """
   records_printed = 0
   ackfile = open_file(file, 'r')

   if limit and not isinstance(limit, int):
       limit = int(limit)

   for record in ackfile.records():
      if limit is None or records_printed < limit:
         print(record)
         records_printed += 1
   print(ackfile)

def version(file):
   """Returns the version of a queues.ack file.

   :param file: the path to the queues.ack file to dump.
   :type file: str
   :returns: the version number of the queues.ack file.
   """
   ackfile = open_file(file, 'r')
   return ackfile.getStoreVersion()

def open_file(path, mode='r', record_size=None):
   """Opens a queues.ack file for reading or writing.
   :param path: the full path to the queues.ack file to open.
   :type path: str
   :param mode: 'r' for reading, 'w' for writing, defaults to 'r'
   :type mode: str
   :param record_size: required when writing; this is the record size used.
   :type record_size: int
   :raises: RuntimeError
   """
   valid_modes = ['r', 'w']
   if mode is None or not mode in valid_modes:
      raise RuntimeError("Unknown mode '%s'" % mode)
   elif mode == 'w' and record_size is None:
      raise RuntimeError("record_size must be supplied when writing a queues.ack file.")
   elif mode == 'r':
      return File(path)
   elif mode == 'w':
      return Writer(path, record_size)

#
# end functions
#
class Record40(dict):
   """Represents a record from an AMPS queues.ack store."""
   def __init__(self, values):
      self.update(values)
   def __str__(self):
      formatstr = """
topic hash         = {topic_hash}
crc                = {crc}
flags              = {flags}
file offset        = {total_offset}
slab offset        = {slab_offset}
allocated          = {allocated}
data length        = {data_length}
generation         = {generation_count}
client             = {client_name_hash}
client seq         = {client_seq}
local txid         = {local_txid}
creation txid      = {creation_txid}
creation_timestamp = {creation_iso_timestamp}
recovery point     = {data}
"""
      return formatstr.format(**self)

class File:
   """Represents a readable AMPS queues.ack file. Use the records() method to extract records."""
   def __init__(self, filename):
      """Constructs self for reading.

      :param filename: the path to the queues.ack file to be read.
      :type filename: str
      """
      self.crc_generator = amps_common.Crc32()
      self.FILE_NAME = filename
      try:
         if(self.FILE_NAME.split('.')[-1] == 'gz'):
            self.file = gzip.open(self.FILE_NAME,'rb')
         else:
            self.file = open(self.FILE_NAME,'rb')
      except IOError as e:
         raise RuntimeError("Error: File '%s' not found.\n" % self.FILE_NAME)
      # First 4096 bytes contains the store metadata header
      self.store_meta_data = amps_common.read_exact(self.file, 4096)
      self.STORE_VERSION = 0.0
      if(self.store_meta_data.find(b'amps-store-v1.0') == 0):
         self.STORE_VERSION = 4.0
         s = struct.Struct('16sQQQQI')
         self.version,self.size,self.record_size,self.increment_size,self.meta_generation_count,self.crc = s.unpack_from(self.store_meta_data)
         self.version = self.version.decode().strip(chr(0))  # filter out the null bytes
      else:
         sys.stderr.write("Error: This file is not in a recognized format.\n")
         sys.stderr.write("Version string reported: %s\n" % self.store_meta_data[0:self.store_meta_data.find(chr(0))][:20] )
      self.offset          = 4096
      self.valid_keys      = 0
      self.max_data_length   = 0
      self.total_data_length = 0
      self.multi_records   = 0
      self.invalid_records = 0
      self.slab_count      = 0
      self.slab_details    = []
   def close(self):
      """Closes the file handle underlying self."""
      self.file.close()
   def getRecordSize(self):
      """Returns the record size as declared in the metadata of this file."""
      return self.record_size
   def getStoreVersion(self):
      """Returns the file store version used to create this file."""
      return self.STORE_VERSION

   def iso8601_time(self, amps_time):
      global IS_LOCALTIME
      if (amps_time == 0):
          return 'unknown'
      """
      Converts amps journal time into an ISO8601 datetime.
      """
      #it = amps_time/1000000 - 210866803200  # subtract the unix epoch
      it = int(Decimal(amps_time)/Decimal(1000000.0) - 210866803200)
      ft = amps_time%1000000
      if not IS_LOCALTIME:
         return time.strftime("%Y%m%dT%H%M%S",time.gmtime(it)) + (".%.6d" % ft)
      else:
         return time.strftime("%Y%m%dT%H%M%S",time.localtime(it)) + (".%.6d" % ft)
      
   def records(self):
      """Generates record objects representing records in this file."""
      if(self.STORE_VERSION == 4.0):
         self.number_of_records = 0
         self.slab_count = 0
         label = struct.Struct('8sQQI')
         s = struct.Struct('IIIIQQQQQQQQ')
         self.read_size = 128
         while(self.file):
            self.file.seek(self.offset)
            self.head = amps_common.read_exact(self.file, self.read_size)
            if (len(self.head) <= 0):
               break
            self.magic,self.slab_size,self.file_offset,self.crc = label.unpack_from(self.head)
            self.slab_detail = {'size':0,'offset':0,'valid_count':0,'invalid_count':0,'stored_bytes':0,'data_bytes':0,'deleted_bytes':0}
            if(self.magic.find(b'AMPSSLAB') != 0):
               #if can't find magic after the first offset then search is done
               if (self.offset > 4096):
                  break
               continue
            self.slab_count = self.slab_count + 1
            self.offset = self.file_offset + self.slab_size
            get = 0
            self.valid_count = 0
            self.invalid_count = 0
            self.stored_bytes = 0
            self.data_bytes = 0
            self.deleted_bytes = 0
            while (get < self.slab_size):
               self.head = amps_common.read_exact(self.file, self.read_size)
               if(len(self.head) <= 0):
                  break
               self.crc,self.flags,self.data_length,self.allocated,self.slab_offset,self.generation_count,self.client_name_hash,self.client_seq,self.local_txid,self.topic_hash,self.creationTxId,self.creationTimestamp = s.unpack_from(self.head)
               if(self.flags > 1 or self.data_length > self.allocated or self.crc == 0):
                  # might be more efficient to seek the file and move to next slab
                  get = get + self.read_size
                  continue
               self.read_offset = get
               get = get + self.allocated
               if(self.data_length == 0 and self.flags == 1):
                  amps_common.read_exact(self.file, self.allocated - self.read_size)
                  self.invalid_records = self.invalid_records + 1
                  self.invalid_count = self.invalid_count + 1
                  self.deleted_bytes = self.deleted_bytes + self.allocated
                  continue
               # if upgrade then eliminte entries that are not required
               if (self.client_seq == 0 and UPGRADE):
                  continue
            # Pick-up the remaining records
               if(self.allocated > 128):
                  self.number_of_records = self.number_of_records + self.allocated / self.record_size
                  self.multi_records = self.multi_records + 1
                  self.head = self.head + amps_common.read_exact(self.file, self.allocated - self.read_size)
               else:
                  self.number_of_records = self.number_of_records + 1
               if(not self.client_name_hash == 0):
                  self.valid_keys = self.valid_keys + 1
                  self.valid_count = self.valid_count + 1
                  self.stored_bytes = self.stored_bytes + self.allocated
                  self.data_bytes = self.data_bytes + self.data_length
                  self.total_data_length = self.total_data_length + self.data_length
                  self.max_data_length = max(self.data_length,self.max_data_length)
               self.data = self.head[s.size:s.size+self.data_length]
               yield Record40({ 'crc':self.crc,
                                'flags': self.flags,
                                'data_length': self.data_length,
                                'allocated': self.allocated,
                                'slab_offset': self.slab_offset,
                                'generation_count': self.generation_count,
                                'client_name_hash': self.client_name_hash,
                                'client_seq': self.client_seq,
                                'local_txid': self.local_txid,
                                'topic_hash': self.topic_hash,
                                'creation_txid': self.creationTxId,
                                'creation_timestamp' : self.creationTimestamp,
                                'creation_iso_timestamp' : self.iso8601_time(self.creationTimestamp),
                                'data': self.data.decode(),
                                'total_offset': self.slab_offset+self.read_offset})
            if (self.slab_size > 0):
               self.slab_detail['size']          = self.slab_size
               self.slab_detail['offset']        = self.file_offset
               self.slab_detail['valid_count']   = self.valid_count
               self.slab_detail['invalid_count'] = self.invalid_count
               self.slab_detail['stored_bytes']  = self.stored_bytes
               self.slab_detail['data_bytes']    = self.data_bytes
               self.slab_detail['deleted_bytes'] = self.deleted_bytes
               self.slab_details.append(self.slab_detail)
   def __str__(self):
      formatstr = """
File                : {0.FILE_NAME}
Version             : {0.version}
Valid Keys          : {0.valid_keys}
Record Size         : {0.record_size}
Maximum records     : {0.number_of_records}
Multirecords        : {0.multi_records}
"""
      retval = formatstr.format(self)
      if(self.STORE_VERSION == 4.0 and len(self.slab_details) > 0):
         formatstr += "Slab Count          : %20s\n" % len(self.slab_details)
         slabformat = """
Slab Detail
 size          : {size:20}
 file offset   : {offset:20}
 valid count   : {valid_count:20}
 invalid count : {invalid_count:20}
 stored bytes  : {stored_bytes:20}
 data bytes    : {data_bytes:20}
 deleted bytes : {deleted_bytes:20}

"""
         for slab_detail in self.slab_details:
            retval += slabformat.format(**slab_detail)
      return retval

class Writer:
   """Represents a writeable queues.ack file. After construction, use the write() method to write records. Make sure to close() this file to finalize the file's metadata."""
   def __init__(self, filename, record_size):
      """Constructs self by creating a new file and writing headers.

      :param filename: the path to the new queues.ack file to be created.
      :type filename: str
      :param record_size: the record size to use for this queues.ack file.
      :type record_size: int
      """
      self.crc_generator = amps_common.Crc32()
      self.record_size = record_size
      self.create_file(filename)

   def fill_gap(self, bytes_to_write):
      self.write_file.write('\0'.ljust(bytes_to_write, '\0'))

   # Create a file for writing an upgraded version
   def create_file(self, filename):
      self.write_filename = filename
      try:
         self.write_file = open(self.write_filename, 'wb')
      except IOError as e:
         sys.stderr.write("Error creating file '%s'.\n" % self.write_filename)
         sys.file.close()
         sys.exit(1)
      self.meta_generation_count = 0
      self.write_version = 'amps-store-v1.0'
      self.WRITE_VERSION = 4.0
      # Skip the metadata area until the end
      self.write_metadata_length = 4096
      self.fill_gap(self.write_metadata_length)
      self.write_size = 0
      self.write_increment_size = 512
      self.flags = int(0)
      self.generation_count = 0
      self.expiration_time = 0
      self.update_time = 0
      self.write_crc = int(0)
      self.write_metadata_crc = int(0)
      self.write_slab_count = 0
      self.write_slab_written = 0
      self.slab_max_size = 1282048
      self.write_label_struct = struct.Struct('8sQQI')
      self.write_record_struct = struct.Struct('IIIIQQQQQQQQ')
      self.write_record_format = 'IIIIQQQQQ{0}s'
      self.write_label = amps_common.FileLabel("AMPSSLAB", 0, 4096, 0)
      self.write_file_size = 0
      self.min_slab_size = 144 * 1024
      # fill in first slab header
      self.write_file.seek(self.write_label.offset)
      self.fill_gap(self.record_size)
      self.write_slab_written = self.record_size
      self.write_file_size = self.write_file_size + self.record_size

   def write(self, rec):
      """Writes a record to this queues.ack file.

      :param rec: the record to write.
      :type rec: dict, with `flags', `data_length', `allocated', `generation_count', `client_name_hash', `client_seq', `local_txid', and `data' fields filled.
      """
      if(self.write_slab_written == 0):
         # new slab, fill header area
         self.write_file.seek(self.write_label.offset)
         self.fill_gap(self.record_size)
         self.write_slab_written = self.record_size
         self.write_file_size = self.write_file_size + self.record_size
      if(self.write_slab_written + rec['allocated'] > self.slab_max_size):
         # Time for a new slab - first close out the previous slab
         self.close_slab()
         self.write_file.seek(self.write_label.offset)
         self.fill_gap(self.record_size)
         self.write_slab_written = self.record_size
         self.write_file_size = self.write_file_size + self.record_size
      self.write_file.seek(self.write_label.offset + self.write_slab_written)
      format = self.write_record_format.format(rec['data_length'])
      record = struct.pack(format,
                           self.write_crc,
                           rec['flags'],
                           rec['data_length'],
                           rec['allocated'],
                           self.write_label.offset,
                           rec['generation_count'],
                           rec['client_name_hash'],
                           rec['client_seq'],
                           rec['local_txid'],
                           rec['topic_hash'],
                           rec['creation_txid'],
                           rec['creation_timestamp'],
                           rec['data'])
      self.write_crc = int(~0) & 0xffffffff
      self.write_crc = self.crc_generator.crc32(record[4:], self.write_crc) & 0xffffffff
      record = struct.pack(format,
                           self.write_crc,
                           rec['flags'],
                           rec['data_length'],
                           rec['allocated'],
                           self.write_label.offset,
                           rec['generation_count'],
                           rec['client_name_hash'],
                           rec['client_seq'],
                           rec['local_txid'],
                           rec['topic_hash'],
                           rec['creation_txid'],
                           rec['creation_timestamp'],
                           rec['data'])
      self.write_file.write(record)
      bytes_to_write = rec['allocated'] - len(record)
      self.fill_gap(bytes_to_write)
      self.write_slab_written = self.write_slab_written + rec['allocated']
      self.write_file_size = self.write_file_size + rec['allocated']

   def close_slab(self):
      if(self.write_slab_written < self.min_slab_size):
         self.write_file.seek(self.write_label.offset + self.write_slab_written)
         bytes_to_write = self.min_slab_size - self.write_slab_written
         self.fill_gap(bytes_to_write)
         self.write_slab_written = self.write_slab_written + bytes_to_write
         self.write_file_size = self.write_file_size + bytes_to_write
      else:
         # Min is a page size, so only check when more than min
         if(self.write_slab_written % 4096 != 0):
            self.write_file.seek(self.write_label.offset + self.write_slab_written)
            bytes_to_write = 4096 - (self.write_slab_written % 4096)
            self.fill_gap(bytes_to_write)
            self.write_slab_written = self.write_slab_written + bytes_to_write
            self.write_file_size = self.write_file_size + bytes_to_write
      self.write_file.seek(self.write_label.offset)
      self.write_label.size = self.write_slab_written
      self.write_label.crc = int(0)
      str = self.write_label_struct.pack(self.write_label.magic, self.write_label.size, self.write_label.offset, self.write_label.crc)
      self.write_label.crc = self.crc_generator.crc32(str[:-4], self.write_label.crc) & 0xffffffff
      str = self.write_label_struct.pack(self.write_label.magic, self.write_label.size, self.write_label.offset, self.write_label.crc)
      self.write_file.write(str)
      self.write_slab_count = self.write_slab_count + 1
      self.write_label.offset = self.write_file_size + 4096
      self.write_label.size = 0
      #self.slab_count = self.slab_count + 1

   def write_metadata(self):
      # Metadata is first 4096 bytes
      self.write_file.seek(0)
      s = struct.Struct('16sQQQQI')
      self.write_metadata_crc = int(0)
      metadata = s.pack(self.write_version,self.write_file_size,self.record_size,self.write_increment_size,self.meta_generation_count,self.write_metadata_crc)
      self.write_metadata_crc = self.crc_generator.crc32(metadata[:-4],self.write_metadata_crc)
      metadata = s.pack(self.write_version,self.write_file_size,self.record_size,self.write_increment_size,self.meta_generation_count,self.write_metadata_crc)
      self.write_file.write(metadata)
      self.fill_gap(128-s.size)

   # Close an open upgrade file
   def close(self):
      """Updates file metadata and closes the file."""
      self.close_slab()
      self.write_metadata()
      self.write_file.close()

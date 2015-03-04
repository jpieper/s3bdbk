#!/usr/bin/env python

'''
s3bdbk.py: A simple script for backing up and restoring block
devices, targeting Amazon S3 like services.

It uses a very simple format in S3, from which the original could be
reconstructed with a shell script if need be.
'''

# Copyright 2011-2015 Josh Pieper, jjp@pobox.com

# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:

# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# TODO:
#  * Verify everything was uploaded before updating the current file
#  * Let configuration file location be specified
#  * Verify SSL is being used


import cStringIO
import datetime
import hashlib
import glob
import gzip
import os
import random
import re
import sys
import time

# The block size should be relatively large.  It can be deduced from
# remote data, so it can be changed without limiting compatibility.
#
# For now, we default it to 32 megs, which is a tradeoff between
# limiting the number of objects in the remote store, and how much we
# have to stick in memory at a time.  (Each block is loaded entirely
# into RAM).
_BLOCK_SIZE = 2**25 # 32Megs

# If there are collisions in this hash function at a block level, data
# will be corrupted.  Thus, it should be relatively strong.
_HASH_FUNCTION = hashlib.sha224

# Versioning information associated with this script.
_BACKUP_VERSION = '1.0'
_S3BDBK_VERSION = '0.2'

class Progress(object):
    def __init__(self, args, operation_name):
        self._verbose = args.verbose
        self._operation_name = operation_name
        self._start = time.time()

    def update(self, count, total_bytes, current_task):
        if not self._verbose:
            return

        if total_bytes == 0:
            complete = 0.0
            eta_string = '0:00:00'
        else:
            complete = float(count) / float(total_bytes)
            so_far = time.time() - self._start
            if complete == 0.0:
                eta_string = '0:00:00'
            else:
                eta = so_far * (1 - complete) / complete
            
                eta_string = '%2d:%02d:%02d' % (
                    eta / 3600, (eta / 60) % 60, eta % 60)
        
        sys.stdout.write('%s:  % 8d /% 8d  - %5.2f%%  %s  %s           \r' % (
                self._operation_name, count, total_bytes,
                100.0 * complete, eta_string, current_task))
        sys.stdout.flush()

class S3Storage(object):
    '''A storage backend based on Amazon S3.'''
    def __init__(self, args):
        import boto
        import boto.s3
        import boto.s3.connection
        import ConfigParser

        # Enable this if you need help debugging boto.
        #
        # boto.set_stream_logger('boto')

        if args.access is None or args.secret is None:
            # Try to get these from a config file.
            parser = ConfigParser.SafeConfigParser()
            found = parser.read([os.path.expanduser('~/.s3cfg')])
                
            args.access = parser.get('default', 'access_key')
            args.secret = parser.get('default', 'secret_key')
            
        assert args.access is not None
        assert args.secret is not None
        assert args.bucket is not None
        assert args.prefix is not None

        self._connection = boto.s3.connection.S3Connection(
            args.access, args.secret)
        self._bucket = self._connection.get_bucket(args.bucket)
        self.prefix = args.prefix

    def exists(self, arg):
        result = self._bucket.get_key(arg)
        return result is not None and result.exists()

    def store(self, name, data, progress_function=None):
        key = self._bucket.new_key(name)
	try:
            key.set_contents_from_string(data, cb=progress_function)
        except:
            sys.stderr.write(
                '\nError when writing name=%s len(data)=%d key=%s\n' % (
                    name, len(data), repr(key)))
            raise

    def load(self, name, progress_function=None):
        key = self._bucket.get_key(name)
        return key.get_contents_as_string(cb=progress_function)

    def list(self, prefix):
        return [x.name for x in self._bucket.list(prefix)]

    def remove(self, name):
        self._bucket.delete_key(name)
        

class DirectoryStorage(object):
    '''A storage backend that just maps to the local filesystem.'''
    def __init__(self, args):
        self.directory, self.prefix = os.path.split(args.directory)
        
        if self.directory == '' and os.path.exists(self.prefix):
            self.directory = self.prefix
        if self.prefix == '':
            self.prefix = self.directory

    def exists(self, arg):
        return os.path.exists(os.path.join(self.directory, arg))

    def store(self, name, data, progress_function=None):
        f = open(os.path.join(self.directory, name), 'wb')
        f.write(data)
        f.close()

    def load(self, name, progress_function=None):
        f = open(os.path.join(self.directory, name), 'rb')
        result = f.read()
        f.close()
        return result

    def list(self, prefix):
        return [os.path.basename(x) for x in
                glob.glob(os.path.join(self.directory, prefix) + '*') ]

    def remove(self, name):
        os.remove(os.path.join(self.directory, name))
        

def make_storage(args):
    '''Create the storage backend appropriate for the given arguments.'''

    if args.directory is not None:
        assert args.access is None
        assert args.secret is None
        assert args.bucket is None
        assert args.prefix is None
        return DirectoryStorage(args)
    
    return S3Storage(args)

def get_canonical_block_name(storage, block_num, name):
    prefix = storage.prefix
    return '%s-data-%08x-%s' % (prefix, block_num, name)

def create_manifest_name(storage):
    prefix = storage.prefix
    return '%s-manifest-%s-%08x' % (
        prefix, datetime.datetime.utcnow().strftime('%Y%m%d-%H%M%S'),
        random.getrandbits(32))

def create_manifest(args, manifest_items):
    header = \
'''Version: %s
Created-by: s3bdbk %s
Block-size: %d
Source: %s
Date: %s
Hash: %s

''' % (_BACKUP_VERSION, _S3BDBK_VERSION, _BLOCK_SIZE, args.block,
       datetime.datetime.utcnow().isoformat(),
       _HASH_FUNCTION().name)
    
    return header + ''.join([item + '\n' for item in manifest_items])

def parse_header(header):
    lines = header.strip().split('\n')
    result = {}
    result.update([(x.strip() for x in line.strip().split(':', 1))
                   for line in lines])
    return result

def parse_manifest(data):
    header, content = data.split('\n\n')
    key_values = parse_header(header)

    assert key_values['Version'] == _BACKUP_VERSION
    block_size = int(key_values['Block-size'])
    
    return content.strip().split('\n'), block_size

def get_current_name(storage):
    prefix = storage.prefix
    return '%s-current' % prefix


def do_backup(args):
    start_time = time.time()
    
    storage = make_storage(args)
    block = open(args.block, 'rb')
    progress = Progress(args, 'backup')

    block.seek(0, os.SEEK_END)
    size = block.tell()
    block.seek(0)

    num_blocks = (size + _BLOCK_SIZE - 1) / _BLOCK_SIZE

    # Iterate through the chunks in this block device and store each
    # one, building up our manifest as we go.

    block_num = 0
    manifest_items = []
    while True:
        progress.update(block_num * _BLOCK_SIZE, size, 'preparing block')
        
        data = block.read(_BLOCK_SIZE)
        if len(data) == 0:
            # all done
            break
        hasher = _HASH_FUNCTION()
        hasher.update(data)
        name = hasher.hexdigest()
        storage_name = get_canonical_block_name(storage, block_num, name)

        if not storage.exists(storage_name):
            dest = cStringIO.StringIO()
            f = gzip.GzipFile(mode='wb', fileobj=dest)
            f.write(data)
            f.close()

            def update_progress(count, total):
                scaled = _BLOCK_SIZE * float(count) / float(total)
                progress.update(block_num * _BLOCK_SIZE + scaled, size,
                                'storing block')
                
            storage.store(storage_name, dest.getvalue(), update_progress)

        manifest_items.append(storage_name)
            
        block_num += 1

        if len(data) < _BLOCK_SIZE:
            break

    manifest_name = create_manifest_name(storage)
    storage.store(manifest_name, create_manifest(args, manifest_items))

    storage.store(get_current_name(storage), manifest_name)

    if args.verbose:
        print ' ' * 75,

    end_time = time.time()
    print "\nWrote backup to: '%s' in %d seconds" % (
        manifest_name, int(end_time - start_time))

    if args.limit is not None:
        do_limit(storage, args)
        
    if args.cleanup:
        do_cleanup(storage, args)
        
    return 0


def do_restore(args):
    storage = make_storage(args)
    progress = Progress(args, 'restore')

    if not args.manifest:
        current_name = get_current_name(storage)
        if not storage.exists(current_name):
            print >>sys.stderr, 'no current backup found'
            return 1

        manifest_name = storage.load(current_name)
    else:
        manifest_name = args.manifest

    if args.verbose:
        print "Restoring from: '%s'" % manifest_name
    manifest = storage.load(manifest_name)

    manifest_items, block_size = parse_manifest(manifest)
    num_blocks = len(manifest_items)

    # First verify that everything exists.
    progress.update(0, 0, 'verifying data')
    for item in manifest_items:
        if not storage.exists(item):
            print >>sys.stderr, "data file '%s' does not exist" % item
            return 1

    if not os.path.exists(args.block):
        block = open(args.block, 'w+b')
    else:
        block = open(args.block, 'r+b')
    
    for block_num, item in enumerate(manifest_items):
        progress.update(block_num * block_size, num_blocks * block_size,
                        'restoring')
        offset = block_size * block_num
        block.seek(offset)

        to_read = block_size

        existing_data = block.read(to_read)
        hasher = _HASH_FUNCTION()
        hasher.update(existing_data)
        name = hasher.hexdigest()

        if get_canonical_block_name(storage, block_num, name) != item:
            def update_progress(count, total):
                scaled = block_size * float(count) / float(total)
                progress.update(block_num * block_size + scaled,
                                num_blocks * block_size,
                                'reading block')
            # We need to restore this data.
            compressed = storage.load(item, update_progress)
            f = gzip.GzipFile(mode='rb', fileobj=cStringIO.StringIO(compressed))
            data = f.read()
            f.close()

            hasher = _HASH_FUNCTION()
            hasher.update(data)
            checksum = hasher.hexdigest()
            if get_canonical_block_name(storage, block_num, checksum) != item:
                print >> sys.stderr, "Checksum error at item '%s'" % item
                return 1
            
            if block_size != len(data):
                print >> sys.stderr, "Size mismatch at item '%s'" % item
                return 1
            block.seek(offset)
            block.write(data)

def do_list(args):
    storage = make_storage(args)

    available = sorted(storage.list(storage.prefix + '-manifest-'))
    print '\n'.join(available) + '\n'

    if args.cleanup:
        do_cleanup(storage, args)
    return 0

def do_version(args):
    print 's3bdbk.py version %s' % _S3BDBK_VERSION
    print 'Copyright 2011-2015 Josh Pieper'
    print
    return 0

def do_cleanup(storage, args):
    if args.verbose:
        print 'Starting cleanup process.'

    # First, check out all the objects we have right now.
    data_files = set(storage.list(storage.prefix + '-data-'))

    # Look to see which ones we care about by aggregative all the
    # active manifests together.
    required_files = set()
    manifests = storage.list(storage.prefix + '-manifest-')
    for manifest_name in manifests:
        items, ignored = parse_manifest(storage.load(manifest_name))
        required_files.update(items)

    data_files.difference_update(required_files)

    # Now data_files contains all the unreferenced data blocks.
    if args.verbose:
        print 'Pruning %d unused data files...' % len(data_files)
    
    for data_file in data_files:
        storage.remove(data_file)
    
    return 0

manifest_re = re.compile('manifest-(\d{8})-(\d{6})-[^-]+$')

def date_from_manifest(manifest):
    m = manifest_re.search(manifest)
    ymd = m.group(1)
    hms = m.group(2)
    return datetime.datetime(int(ymd[0:4]), int(ymd[4:6]), int(ymd[6:8]),
                             int(hms[0:2]), int(hms[2:4]), int(hms[4:6]))

def total_seconds(td):
    # We could just use timedelta.total_seconds() if this was python
    # 2.7 or better.
    return ((td.microseconds +
             (td.seconds + td.days * 24 * 3600) * 10**6) / float(10**6))

def calculate_manifest_weight(most_recent, previous, current):
    previous_date = date_from_manifest(previous)
    current_date = date_from_manifest(current)
    delta = abs(current_date - previous_date)
    dt = total_seconds(delta)

    recent_date = date_from_manifest(most_recent)
    age = total_seconds(abs(recent_date - current_date))
    if dt == 0:
        return 0.0
    return (1 / dt) ** 2 * (age ** 0.5)

def weighted_choice_sub(weights):
    rnd = random.random() * sum(weights)
    for i, w in enumerate(weights):
        rnd -= w
        if rnd < 0:
            return i

def select_manifest_to_remove(manifests):
    # Apply a weighted random selection process.  The weight for each
    # manifest is:
    #
    # (1/t)**2
    #  t=time in seconds since next prior backup

    manifests = sorted(manifests)
    weights = [calculate_manifest_weight(
            manifests[-1], manifests[i], manifests[i - 1])
               for i in range(1, len(manifests) - 1)]

    # We cannot remove the first or last manifest.
    manifests = manifests[1:-1]

    index = weighted_choice_sub(weights)
    return manifests[index]

    

def do_limit(storage, args):
    limit = int(args.limit)
    
    manifests = sorted(storage.list(storage.prefix + '-manifest-'))
    if len(manifests) < limit:
        return

    print 'Existing backups exceed limit, %d > %d' % (
        len(manifests), limit)
    if args.verbose:
        print ''.join(['  %s\n' % x for x in manifests])

    while len(manifests) > limit:
        to_remove = select_manifest_to_remove(manifests)
        print "Purging old backup manifest: '%s'" % to_remove
        storage.remove(to_remove)
        manifests.remove(to_remove)
        

def main():
    import optparse
    parser = optparse.OptionParser(
        description='Save/restore block devices to remote storage.')
    parser.add_option('-v','--verbose', action='store_true',
                      help='display additional information')
    parser.add_option('-b','--block', help='block device')
    parser.add_option('-d','--directory',
                        help='destination directory (not S3)')
    parser.add_option('--manifest', help='restore from a specific manifest')
    parser.add_option('--cleanup', action='store_true',
                      help='purge unreferenced blocks ' +
                      '(only after backup/list)')
    parser.add_option('-l', '--limit', help='purge to keep no more than ' +
                      'this many backups')
    
    parser.add_option('--access', help='S3 Access Key')
    parser.add_option('--secret', help='S3 Secret Key')
    parser.add_option('--bucket', help='S3 Bucket')
    parser.add_option('--prefix', help='prefix in S3 bucket')

    cmd_group = optparse.OptionGroup(parser, "Sub-commands")
    cmd_group.add_option('--backup', action='append_const',
                         const=do_backup, dest='func',
                         help='save block device to remote storage')
    cmd_group.add_option('--restore', action='append_const',
                         const=do_restore, dest='func',
                         help='restore block device from remote storage')
    cmd_group.add_option('--list', action='append_const',
                         const=do_list, dest='func',
                         help='list existing backups')
    cmd_group.add_option('--version', action='append_const',
                         const=do_version, dest='func',
                         help='display version information')
    parser.add_option_group(cmd_group)

    (args, extra) = parser.parse_args()

    if args.func is None:
        print 'No sub-commands were specified.'
        print 
        parser.print_help()
        return 1
    
    if len(args.func) > 1:
        print 'Only one sub-command may be specified.'
        print
        parser.print_help()
        return 1

    result = args.func[0](args)
    
    if args.verbose:
        sys.stdout.write(' ' * 75 + '\r')
        sys.stdout.flush()
    return result
    
if __name__ == '__main__':
    sys.exit(main())
    

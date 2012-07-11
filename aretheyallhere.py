#!/usr/bin/env python3
"""
    Copyright (C) 2012 Francois Truphemus (melchips@kingsofpluton.net)

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

USAGE_MORE_INFO = """
    This script feeds a sqlite database with files and their checksums (SHA1 by default) from two different folders and subfolders.
    It can be used to tell if all files of the source directory are in the destination folder, whatever their path may be (the comparison is made by checking the missing checksums in destination)
"""

import os
import sys
import datetime
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Binary, LargeBinary, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import buffer
import hashlib
import argparse

DATABASE_FILE = 'aretheyallhere.db'

# For a fancy text animation, uncomment the one that you like the most ^^
#TEXT_ANIMATION = "▁▂▃▄▅▆▇█▇▆▅▄▃▂"
#TEXT_ANIMATION = "○◔◑◕●"
#TEXT_ANIMATION = "◓◑◒◐" 
#TEXT_ANIMATION = "⇐⇖⇑⇗⇒⇘⇓⇙"
#TEXT_ANIMATION = ".o0O0o. "
TEXT_ANIMATION = "|/-\\"
#TEXT_ANIMATION = "▏▎▍▌▋▊▉█▉▊▌▍▎"
#TEXT_ANIMATION = "▖▘▝▗"
#TEXT_ANIMATION = "┤┘┴└├┌┬┐"
#TEXT_ANIMATION = "◢◣◤◥"
#TEXT_ANIMATION = "◰◳◲◱"
#TEXT_ANIMATION = "◡◡⊙⊙◠◠"
#TEXT_ANIMATION = "⣾⣽⣻⢿⡿⣟⣯⣷"
#TEXT_ANIMATION = "⠁⠂⠄⡀⢀⠠⠐⠈"
#TEXT_ANIMATION = ".oOo"
#TEXT_ANIMATION = ".oO°Oo."

# Converting the text animation string to a list
TEXT_ANIMATION=[c for c in TEXT_ANIMATION]

parser = argparse.ArgumentParser(description=USAGE_MORE_INFO)

parser.add_argument('-f', '--force', dest='force_overwrite', action='store_const', const=True, default=False, help='force overwriting the content of the database file')
parser.add_argument('-db', '--database', metavar='database_file', dest='database_file', type=str, default=DATABASE_FILE, help='specify database file to be used by this app (default is file "' + DATABASE_FILE + '" in current path)')
parser.add_argument('-c', '--checksum-type', dest='checksum_type', choices=('sha1','md5'), default='sha1', help='set checksum algorithm to be used for comparing files')
parser.add_argument('-s','--source', metavar='path_source', dest='path_source', help='source path to be used for comparison')
parser.add_argument('-d','--destination', metavar='path_destination', dest='path_destination', help='destination path in which we try to find files of source path')
args = parser.parse_args()

engine = create_engine(
        'sqlite:///%s' % args.database_file,
        echo=False)
base = declarative_base()

# FileRecord in sqlite database
class FileRecord(base):
    __tablename__ = 'filerecord'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    path = Column(String)
    checksum = Column(String)
    mimetype = Column(String)
    referential = Column(String)

    def __init__(self, name, path, checksum, mimetype, referential):
        self.name = name
        self.path = path
        self.checksum = checksum
        self.mimetype = mimetype
        self.referential = referential
       
    def __repr__(self):
        return "file:%s fullpath:%s checksum:%s referential:%s mime:%s" % (self.name, self.path, self.checksum, self.referential, self.mimetype)

base.metadata.create_all(engine)

# The main app
class AreTheyAllHereApp:
    def __init__(self, path_source, path_destination, checksum_type):
        self.path_source = path_source
        self.path_destination = path_destination
        self.init_database()
        self.text_anim_state = 0
        self.text_anim = TEXT_ANIMATION
        self.text_anim_last_text_length = 0
        self.total_files_in_source = 0
        self.total_files_in_destination = 0
        self.total_files_size_in_source = 0
        self.total_files_size_in_destination = 0
        self.compute_time_start = 0
        self.processed_files_count = 0
        self.processed_size_count = 0
        self.checksum_type = checksum_type
        
    # database initialization
    def init_database(self):
        self.engine = engine
        self.base = base
        self.session = sessionmaker(bind=engine)()

    # compute checksum from file
    def get_file_checksum(self, filepath):
        checksum = ''
        fp = open(filepath, 'rb')
        if fp:
            if self.checksum_type == 'sha1':
                checksum = hashlib.sha1(fp.read()).hexdigest()
            elif self.checksum_type == 'md5':
                checksum = hashlib.md5(fp.read()).hexdigest()
            else:
                checksum = hashlib.md5(fp.read()).hexdigest()
        fp.close()
        return checksum

    def text_progress_anim(self, additionnal_text):
        string_to_write = "%s %s" % ( self.text_anim[self.text_anim_state], additionnal_text)
        sys.stdout.write('\r' + string_to_write + (' ' * (self.text_anim_last_text_length - len(string_to_write)) ))
        sys.stdout.flush()
        self.text_anim_last_text_length = len(string_to_write)
        self.text_anim_state+=1
        if self.text_anim_state >= len(self.text_anim):
            self.text_anim_state = 0

    def text_progress_anim_erase(self):
        sys.stdout.write('\r' + (' ' * self.text_anim_last_text_length) + '\r')
        sys.stdout.flush()
    
    # populate database from a given directory and its subdirectories
    def scan_and_populate_from_path(self, path, referential_name):
        for r,d,f in os.walk(path):
            for files in f:
                filepath = os.path.join(r, files)
                files_left_to_be_processed = (self.total_files_in_source + self.total_files_in_destination - self.processed_files_count)
                size_left_to_be_processed = (self.total_files_size_in_source + self.total_files_size_in_destination - self.processed_size_count)
                if self.processed_files_count > 0: 
                    remaining_time = (datetime.datetime.now() - self.compute_time_start)/self.processed_files_count * files_left_to_be_processed
#                    remaining_time = (datetime.datetime.now() - self.compute_time_start)/self.processed_size_count * size_left_to_be_processed
                else:
                    remaining_time = datetime.timedelta()
                self.text_progress_anim("processing file %d/%d from referential %s (%s remaining)" % (self.processed_files_count, self.total_files_in_source + self.total_files_in_destination, referential_name, self.get_remaining_time_as_string(remaining_time) ))
                filechecksum = self.get_file_checksum(filepath)
                filesize = os.path.getsize(filepath)
                filename = files
                # TODO : extract mime info
                filemimetype = '?'
                filereferential = referential_name
                filerecord = FileRecord(filename, filepath, filechecksum, filemimetype, filereferential) 
                self.session.add(filerecord)
                self.session.commit()
                self.processed_files_count += 1
                self.processed_size_count += filesize
                self.compute_time_end = datetime.datetime.now()
                self.compute_time_last_total = self.compute_time_end - self.compute_time_start

    # scan given directories and populate database with it
    def populate_database(self):
        if self.path_source != None:
            self.total_files_in_source, self.total_files_size_in_source = self.get_total_count_and_size_of_files_in_path(self.path_source)
        else:
            self.total_files_in_source = 0
            self.total_files_size_in_source = 0

        if self.path_destination != None:
            self.total_files_in_destination, self.total_files_size_in_destination = self.get_total_count_and_size_of_files_in_path(self.path_destination)
        else:
            self.total_files_in_destination = 0
            self.total_files_size_in_destination = 0

        self.compute_time_start = datetime.datetime.now()
        self.processed_files_count = 0
        self.processed_size_count = 0

        if self.path_source != None:
            self.scan_and_populate_from_path(self.path_source, 'source')
        if self.path_destination != None:
            self.scan_and_populate_from_path(self.path_destination, 'destination')
        self.text_progress_anim_erase()

    # returns list of missing source files in destination folder
    def get_missing_source_files_in_destination(self):
        # select * from filerecord where referential='source' and checksum not in(select checksum from filerecord where referential='destination');
        files_count = 0
        for files in self.session.query(FileRecord).filter(FileRecord.referential == 'source').filter(~FileRecord.checksum.in_(self.session.query(FileRecord.checksum).filter(FileRecord.referential == 'destination'))):
            print(files)
            files_count += 1
        print("Total = %d file(s)" % files_count)

    # Check if database is empty
    def is_database_empty(self):
        if self.session.query(FileRecord).count() == 0:
            return True
        else:
            return False

    def get_total_count_and_size_of_files_in_path(self, path):
        file_count = 0
        total_size = 0
        for root, dirs, files in os.walk(path):
            self.text_progress_anim("counting files in path %s" % path)
            file_count += len(files)
            for  f in files:
                fp = os.path.join(root, f)
                total_size += os.path.getsize(fp)
        self.text_progress_anim_erase()
        return (file_count, total_size)

    def get_remaining_time_as_string(self, timedelta):
        output = ''
        hours, remain = divmod(timedelta.seconds, 3600)
        minutes, seconds = divmod(remain, 60)

        if timedelta.days == 1:
            output += '1 day, '
        elif timedelta.days > 1:
            output +=  '%d days, ' % timedelta.days
        if hours == 1:
            output += '1 hour, '
        elif hours > 1:
            output += '%d hours, ' % hours
        if minutes == 1:
            output += '1 minute and '
        elif minutes > 1:
            output += '%d minutes and ' % minutes
        if seconds == 1:
            output += '1 second'
        else:
            output += '%d seconds' % seconds
        return output

if __name__ == "__main__":
    myapp = AreTheyAllHereApp(args.path_source, args.path_destination, args.checksum_type)
    if (myapp.is_database_empty() or args.force_overwrite):
        myapp.populate_database()
    elif args.path_source != None or args.path_destination != None :
        print("Warning : As the database '%s' is not empty, scanning is skipped to avoid overwriting. Please delete manually the database file to force scanning or add the '--force' option." % args.database_file)
    print("List of missing file(s) in destination :")
    myapp.get_missing_source_files_in_destination()
    sys.exit(0)

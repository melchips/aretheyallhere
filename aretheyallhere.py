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
    This script feeds a sqlite database with files and their checksums (SHA1) from two different folders and subfolders

    Purpose:
    It can be used to tell if all files of the source directory are in the destination folder, whatever their path may be (the comparison is made by checking the missing checksums in destination)
"""

import os
import sys
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Binary, LargeBinary, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import buffer
import hashlib

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

def usage():
    print("Usage: %s path_source_of_comparison path_dest_of_comparison\n%s" % (sys.argv[0],USAGE_MORE_INFO) )

if (len(sys.argv)!=3):
    usage()
    sys.exit(2)

PATH_SOURCE = sys.argv[1]
PATH_DESTINATION = sys.argv[2]

engine = create_engine(
        'sqlite:///%s' % DATABASE_FILE,
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
    def __init__(self, path_source, path_destination):
        self.path_source = path_source
        self.path_destination = path_destination
        self.init_database()
        self.text_anim_state = 0
        self.text_anim = TEXT_ANIMATION
        self.text_anim_last_text_length = 0
        
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
            checksum = hashlib.sha1(fp.read()).hexdigest()
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
                self.text_progress_anim("processing file " + filepath)
                filechecksum = self.get_file_checksum(filepath)
                filename = files
                # TODO : extract mime info
                filemimetype = '?'
                filereferential = referential_name
                filerecord = FileRecord(filename, filepath, filechecksum, filemimetype, filereferential) 
                self.session.add(filerecord)
                self.session.commit()

    # scan given directories and populate database with it
    def populate_database(self):
        self.scan_and_populate_from_path(self.path_source, 'source')
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

if __name__ == "__main__":
    myapp = AreTheyAllHereApp(PATH_SOURCE, PATH_DESTINATION)
    if (myapp.is_database_empty()):
        myapp.populate_database()
    else:
        print("Warning : As the database '%s' is not empty, scanning is skipped to avoid overwriting. Please delete manually the database file to force scanning." % DATABASE_FILE)
    print("List of missing file(s) in destination :")
    myapp.get_missing_source_files_in_destination()
    sys.exit(0)

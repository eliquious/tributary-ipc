from tributary.streams import StreamProducer
from tributary.core import Message
from tributary.utilities import validateType

import os, glob, re

__all__ = ['FileStream', 'RecursiveFileStream', 'GlobFileStream', 'DelimFileStream']

class FileStream(StreamProducer):
    """FileStream is a base class for all data sources which come from files."""
    def __init__(self, name, filename):
        super(FileStream, self).__init__(name)
        self.name = name
        self.filename = filename

class RecursiveFileStream(FileStream):
    """Recursively fetches a list of files from a directory."""
    def __init__(self, name, root_dir):
        super(RecursiveFileStream, self).__init__(name, root_dir)

    def _add_file(self, arg, dirname, fnames):
        for name in fnames:
            self.scatter(Message(filename="%s/%s" % (dirname, name)))
            self.tick()

    def process(self, msg):
        os.path.walk(self.filename, self._add_file, 0)

class GlobFileStream(FileStream):
    """GlobStream fetches all files matching the glob string"""
    def __init__(self, name, glob_string):
        super(GlobFileStream, self).__init__(name, glob_string)

    def process(self, msg):
        self.log("Globbing %s"% self.filename)
        filenames = glob.glob(self.filename)
        self.log("Found %i matching files" % len(filenames))
        for filename in filenames:
            self.scatter(Message(filename=os.path.abspath(filename)))


class DelimFileStream(FileStream):
        """Basic data source for delimited files. Supports both structured and unstructured data files.

                :param name: Name of the processing node
                :type name: str
                :param filename: Name of file to be processed
                :type filename: str
                :param delim: Field separator (default - ",")
                :type delim: str
                :param cols: List of column names (None - columns are auto-named)
                :type cols: List or Tuple
                :param skip_pre_header_lines: Number of lines to skip before reading header (has_header can be false)
                :type skip_pre_header_lines: int
                :param skip_post_header_lines: Number of lines to skip after reading header (has_header can be false)
                :type skip_post_header_lines: int
                :param has_header: Does the file have a header?
                :type has_header: bool
                :param comment_char: Lines that start with this are skipped
                :type comment_char: str
                :param skip_lines_without_delim: Skip lines that do not contain the specified delimiter
                :type skip_lines_without_delim: bool

                .. note::

                        * If both `skip_pre_header_lines` and `skip_post_header_lines` are specified and has_header is **False**, then the summation of them is skipped.
                        * If `cols` is given, then `has_header` is automatically set to **False**.
                        * `comment_char` can be set to None to forfeit the functionality.
                        * If `cols` is **None** and `has_header` is **False**, the data is loaded as 'Column_1', 'Column_2', etc.
        """
        def __init__(self, name, filename, delim=",", cols=None, skip_pre_header_lines=0,
                        skip_post_header_lines=0, has_header=True, comment_char="#", skip_lines_without_delim=True, batch=False):
            super(DelimFileStream, self).__init__(name, filename)

            self.delim = delim
            self.cols = cols
            self.skip_pre_header_lines = skip_pre_header_lines
            self.skip_post_header_lines = skip_post_header_lines
            self.has_header = has_header
            self.comment_char = comment_char
            self.skip_lines_without_delim = skip_lines_without_delim
            self.batch = batch

            if cols is not None:
                self.has_header = False

        def split(self, line):
            if line is None:
                return ""
            if self.delim is not None:
                return re.split(self.delim, line.strip())
            else:
                return line.strip().split()

        def process(self, msg):

            # open file
            with open(self.filename, "r") as fh:

                # skip pre header lines
                if self.skip_pre_header_lines > 0:
                    for i in xrange(self.skip_pre_header_lines):
                        fh.readline()

                #read header
                if self.has_header == True:
                    self.cols = [col.strip() for col in self.split(fh.readline())]

                # self.results.cols = self.cols #why was this line before the 'with' block? --andy

                # skip post header lines
                if self.skip_post_header_lines > 0:
                    for i in xrange(self.skip_post_header_lines):
                        fh.readline()

                # compile regex of delim
                regexp = re.compile(self.delim)
                
                if self.batch:
                    results = []

                # read lines in file
                for line in fh:

                    # skip comments if applicable
                    if not self.comment_char is None and line.strip().startswith(self.comment_char):
                        continue

                    # skip lines without delim if `skip_lines_without_delim` is True

                    if self.delim is not None:
                        if self.skip_lines_without_delim is True and not regexp.search(line):
                        # if self.skip_lines_without_delim is True and not self.delim in line:
                            continue

                    # init data message, read fields and count them
                    data = {}
                    fields = self.split(line.strip())
                    field_count = len(fields)

                    # determine what to call the data fields
                    if self.cols is not None:
                        for i, col in enumerate(self.cols):
                            if i < field_count:
                                data[col] =  fields[i].strip() #I don't think it'll harm anything to strip whitespace here
                            else:
                                data[col] = None
                    else:

                        # for unstructured data, cols is None and has_header is False
                        for i, field in enumerate(fields):
                            data["Column_" + str(i + 1)] = fields[i].strip() #I don't think it'll harm anything to strip whitespace here

                    if not self.batch:
                        # send message
                        self.scatter(Message(**data))
                    else:
                        results.append(Message(**data))

                if self.batch:
                    self.emitBatch('data', results)



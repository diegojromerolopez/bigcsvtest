import os
import platform
import tempfile
import time
import uuid
import csv

import shutil

from multiprocessing import Process, Pipe, Queue

from csv_worker import CsvWorker

try:
    from urllib.request import Request, urlopen  # Python 3
except:
    from urllib2 import Request, urlopen  # Python 2


class RemoteFileReader(object):

    def __init__(self, url, debug=True):
        """
        Create a new RemoteFileReader
        :param url: HTTP url where the file is in.
        :param debug: Should we show debug information, or not?
        """
        super(RemoteFileReader, self).__init__()
        self.url = url
        self.file_size = None
        self.file_name = None
        self.num_lines = None
        self.end_time = 0
        self.start_time = 0
        self.debug = debug
        # Custom
        if debug:
            self.print = lambda x: print(x)
        else:
            self.print = lambda x: x

    # Gets the size from the header of the response
    def size(self):
        d = urlopen(self.url)
        self.file_size = int(d.getheader('Content-Length'))
        return self.file_size

    @property
    def is_fetched(self):
        return self.file_name is not None

    # Fetch the file if needed
    # Returns the name of the file
    def fetch(self, num_processes=100, chunk_size=16*1024):
        """
        Fetch the file in the filesystem. 
        :param num_processes: Number of processes to be used to fetch the file 
        :param chunk_size: Default chunk size when each process write the file.
        :return: None
        """
        # In case we already have fetched the file, get out
        if self.file_name:
            return True

        # Get the file size if it has not been already calculated
        if self.file_size is None:
            self.size()

        # File must not be empty
        if self.file_size == 0:
            raise AssertionError("File is empty")

        # TMP dir
        tmp_dir = "/tmp" if platform.system() == 'Darwin' else tempfile.gettempdir()

        # File name
        self.file_name = "{0}/{1}.csv".format(tmp_dir, uuid.uuid4())
        self.print("{0} is the file path of the download file".format(self.file_name))

        # Worker function for each process
        def download(_process_name, _send_end, _url, _file_name, _byte_range):
            self.print("{0} with byte range {1}".format( _process_name, _byte_range, (_byte_range[1]-_byte_range[0]) ))

            # Request the URL
            q = Request(_url)

            # Request only a byte range
            # Reference: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
            q.add_header('Range', 'bytes={0}-{1}'.format(_byte_range[0], _byte_range[1]))

            # Storing the number of lines that this file has
            num_lines = 0

            # Writing chunks of text because it is more efficient than writing
            # at the end once
            response = urlopen(q)
            readed_bytes = 0
            with open(_file_name, 'wb') as _f:
                chunk_i = 0
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    num_lines += chunk.decode("utf-8").count('\n')
                    _f.write(chunk)
                    try:
                        readed_bytes += len(chunk)
                    except Exception:
                        pass
                    chunk_i += 1

            _send_end.send(num_lines)
            self.print("{0} has completed the download. {1} num lines and {2} readed bytes!".format(
                _process_name, num_lines, readed_bytes)
            )

        bytes_per_process = self.file_size // num_processes

        num_lines_list = []
        processes = []
        processes_file_names = []
        for i in range(0, num_processes):
            recv_end, send_end = Pipe(False)
            file_i = "{0}-{1}".format(self.file_name, i+1)
            processes_file_names.append(file_i)
            byte_range_start = i * bytes_per_process
            byte_range_end = byte_range_start + bytes_per_process - 1
            byte_range = (byte_range_start, byte_range_end)
            process = Process(
                target=download, args=("Process {0}".format(i+1), send_end, self.url, file_i, byte_range)
            )
            processes.append(process)
            num_lines_list.append(recv_end)
            self.print("Process {0} prepared".format(i+1))

        i = 0
        for process in processes:
            self.print("Process {0} started".format(i + 1))
            process.start()
            i += 1

        i = 0
        for process in processes:
            process.join()
            self.print("Process {0} ended".format(i + 1))
            i += 1

        self.num_lines = sum([num_lines.recv() for num_lines in num_lines_list])
        self.print("File has {0} lines".format(self.num_lines))

        with open(self.file_name, 'wb') as file_:
            for process_file_name in processes_file_names:
                with open(process_file_name, 'rb') as process_file:
                    shutil.copyfileobj(process_file, file_, 1024 * 1024 * 10)
                os.remove(process_file_name)

    # Remove the original file
    def remove(self):
        if self.file_name:
            os.remove(self.file_name)
            self.file_name = None
            return True
        return False

    # Some float column values are not valid,
    # For example, they contain two dots instead of one
    # If there were any other data cleaning to do, it would be included here
    @staticmethod
    def _to_float(value):
        return float(value)

    # Compute the average value of a column
    def avg(self, field_name):
        if self.file_name is None:
            raise ValueError("CSV file is not fetched")

        worker = CsvWorker(self.file_name)
        tip_amount_sum, read_lines = worker.sum_column(
            column_name="tip_amount", num_processes=16, num_lines=self.num_lines
        )
        return float(tip_amount_sum) / float(self.num_lines)

    def start(self):
        self.start_time = time.time()

    def end(self):
        self.end_time = time.time()

    @property
    def elapsed_time(self):
        return self.end_time - self.start_time




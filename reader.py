import os
import time
import uuid
import csv

import shutil

from multiprocessing import Process, Pipe

try:
    from urllib.request import Request, urlopen  # Python 3
except:
    from urllib2 import Request, urlopen  # Python 2


class Reader(object):

    def __init__(self, url, debug=True):
        super(Reader, self).__init__()
        self.url = url
        self.file_size = None
        self.file_name = None
        self.num_lines = None
        self.end_time = 0
        self.start_time = 0
        self.debug = debug
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
    def fetch(self, num_processes=100):
        if self.file_name:
            return True

        if self.file_size is None:
            self.size()

        self.file_name = "/tmp/{0}.csv".format(uuid.uuid4())

        # Worker function for each process
        def download(_process_name, _url, _file_name, _byte_range):
            self.print("{0} with byte range {1}".format( _process_name, _byte_range, (_byte_range[1]-_byte_range[0]) ))
            q = Request(_url)
            # Request only a byte range
            # Reference: https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
            q.add_header('Range', 'bytes={0}-{1}'.format(_byte_range[0], _byte_range[1]))
            # Writing chunks of text because it is more efficient than writing
            # at the end once
            response = urlopen(q)
            chunk_size = 16 * 1024
            with open(_file_name, 'wb') as _f:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    _f.write(chunk)
            self.print("{0} has completed the download!".format(_process_name))

        bytes_per_process = self.file_size // num_processes

        processes = []
        processes_file_names = []
        for i in range(0, num_processes):
            file_i = "{0}-{1}".format(self.file_name, i+1)
            processes_file_names.append(file_i)
            byte_range_start = i * bytes_per_process
            byte_range_end = byte_range_start + bytes_per_process - 1
            byte_range = (byte_range_start, byte_range_end)
            process = Process(target=download, args=("Process {0}".format(i+1), self.url, file_i, byte_range))
            processes.append(process)
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
        field_name_sum = worker.reduce_field(field_name=field_name, operation=lambda x, y: x+float(y))

        return float(field_name_sum) / float(worker.num_lines)

    def start(self):
        self.start_time = time.time()

    def end(self):
        self.end_time = time.time()

    @property
    def elapsed_time(self):
        return self.end_time - self.start_time


class CsvWorker(object):

    def __init__(self, file_name):
        self.file_name = file_name
        self.num_lines = 0

    def reduce_field(self, field_name, operation, condition=None):
        with open(self.file_name, "r") as csv_file:
            csv_reader = csv.DictReader(csv_file)
            acc = 0.0
            self.num_lines = 0
            row_index = 0
            for row in csv_reader:
                if row[field_name] and ((condition and condition(row_index, row)) or not condition):
                    acc = operation(acc, row[field_name])
                    self.num_lines += 1
                row_index += 1
        return acc

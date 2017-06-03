

# CSV worker
import csv

import time
from multiprocessing.dummy import Pool


class CsvWorker(object):

    def __init__(self, file_name):
        """
        Creates a new CsvWorker.
        :param file_name: File name of the CSV file to be processed.
        """
        self.file_name = file_name

    def sum_column(self, column_name, condition=None, num_processes=4, num_lines=100):
        """
        Sum a column of the CSV file.
        :param column_name: File the reduction will be based on. 
        :param condition: Condition that all the rows : row_index, row => True or False. Optional.
        :return: Result of the reduce operation.
        """

        def work(limits_):
            _start_line = limits_["start_line"]
            _end_line = limits_["end_line"]
            print("{} to {}".format(_start_line, _end_line))
            with open(self.file_name, "r") as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for offset in range(0, _start_line):
                    try:
                        next(csv_reader)
                    except StopIteration:
                        return {"sum": 0.0, "number_of_read_lines": 0}

                acc = 0.0
                row_index = _start_line
                while row_index <= _end_line:
                    try:
                        row = next(csv_reader)
                    except StopIteration:
                        return {"sum": 0.0, "number_of_read_lines": row_index - _start_line}
                    if row[column_name] and (condition is None or (condition and condition(row_index, row))):
                        acc = acc + float(row[column_name])
                    row_index += 1

            return {"sum": acc, "number_of_read_lines": row_index - _start_line}

        num_lines_per_processor = num_lines // num_processes

        process_data = []

        i = 0
        end_line = 0
        while end_line < num_lines:
            start_line = num_lines_per_processor * i + 1
            end_line = start_line + num_lines_per_processor - 1
            process_data.append({"start_line": start_line, "end_line": end_line})
            i += 1

        pool = Pool(processes=num_processes)
        res = pool.map_async(work, (pd for pd in process_data))
        results = res.get()
        column_sum = 0.0
        number_of_read_lines = 0
        for item in results:
            column_sum += item["sum"]
            number_of_read_lines += item["number_of_read_lines"]

        return column_sum, number_of_read_lines


if __name__ == "__main__":
    start_time = time.time()
    worker = CsvWorker("~/Desktop/yellow_tripdata_2016-01.csv")
    tip_amount_sum, read_lines = worker.sum_column(
        column_name="tip_amount", num_processes=16, num_lines=10000000
    )
    end_time = time.time()
    print("Sum of tip_amount is {0}".format(tip_amount_sum))
    print("Number of read lines is {0}".format(read_lines))
    print("Elapsed time: {0} s".format(end_time - start_time))

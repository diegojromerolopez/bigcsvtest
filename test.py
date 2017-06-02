
# Your code should:
#
# Download this 2.2GB file: https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv
# Count the lines in the file
# Calculate the average value of the tip_amount field.

from reader import Reader

if __name__ == "__main__":
    print("Starting program")

    reader = Reader("https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv", debug=True)

    reader.start()
    reader.fetch(num_processes=45)
    reader.end()
    print("Fetched in {0} s".format(reader.elapsed_time))

    reader.start()
    avg_tip_amount = reader.avg("tip_amount")
    reader.end()

    print("Avg. tip amount {0}".format(avg_tip_amount))
    print("Num lines {0}".format(reader.num_lines))

    print("Elapsed time: {0} s".format(reader.elapsed_time))

from mrjob.job import MRJob
import csv
import math
from io import StringIO

class LapTimeAverage(MRJob):
    """Simple MapReduce job computing average lap time per driver."""

    def mapper(self, _, line):
        if line.startswith('Driver'):
            return
        reader = csv.reader(StringIO(line))
        fields = next(reader)
        if len(fields) < 3:
            return
        driver = fields[0]
        try:
            lap_time = float(fields[2])
        except ValueError:
            return
        if math.isnan(lap_time):
            return
        yield driver, (lap_time, 1)

    def reducer(self, driver, values):
        total = 0.0
        count = 0
        for lap_time, num in values:
            if math.isnan(lap_time):
                continue
            total += lap_time
            count += num
        if count:
            yield driver, total / count

if __name__ == '__main__':
    LapTimeAverage.run()
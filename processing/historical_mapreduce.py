from mrjob.job import MRJob
import csv
from mrjob.step import MRStep
from io import StringIO
import math

class LapTimeStats(MRJob):
    """Compute average and fastest lap times per driver and tyre compound."""
   
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_stats),
            MRStep(reducer=self.reducer_format)
        ]
    
    def mapper(self, _, line):
        if line.startswith('Driver'):
            return
        reader = csv.reader(StringIO(line))
        fields = next(reader)
        if len(fields) < 6:
            return
        driver = fields[0]
        try:
            lap_time = float(fields[2])
        except ValueError:
            return
        if math.isnan(lap_time):
            return
        compound = fields[5] or 'UNKNOWN'
        yield (driver, compound), (lap_time, 1, lap_time)
        yield (driver, 'ALL'), (lap_time, 1, lap_time)

    def combiner(self, key, values):
        total, count, fastest = 0.0, 0, float('inf')
        for t, c, f in values:
            total += t
            count += c
            fastest = min(fastest, f)
        yield key, (total, count, fastest)

    def reducer_stats(self, key, values):
        total, count, fastest = 0.0, 0, float('inf')
        for t, c, f in values:
            total += t
            count += c
            fastest = min(fastest, f)
        yield key, (total, count, fastest)

    def reducer_format(self, key, values):
        for total, count, fastest in values:
            avg = total / count if count else 0
            driver, compound = key
            yield None, f"{driver},{compound},{avg:.3f},{fastest:.3f}"

if __name__ == '__main__':
    LapTimeStats.run()
import csv

with open("race.csv", "r") as infile, open("race_fixed.csv", "w", newline='') as outfile:
    reader = csv.DictReader(infile)
    writer = csv.writer(outfile)
    writer.writerow(["Driver", "LapNumber", "LapTimeSeconds", "TrackStatus", "Position", "Compound", "TyreLife", "FreshTyre"])

    for row in reader:
        try:
            minutes, seconds = row["time"].split(":")
            lap_time_sec = float(minutes) * 60 + float(seconds)
            writer.writerow([
                row["driver"], "1", lap_time_sec, "", row["position"], "", "", ""
            ])
        except Exception as e:
            print("Skipping row due to error:", e)

import datetime

some = datetime.datetime.strptime("2023-10-30 16:26:02.802165", "%Y-%m-%d %H:%M:%S.%f").isocalendar()[1]
print(some)
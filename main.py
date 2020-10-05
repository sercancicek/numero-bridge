import sys
import schedule
import time
from remote_reader import read_data


def build_the_bridge():
    read_data()


TICK = 60
if len(sys.argv) > 1:
    TICK = sys.argv[1]


# After every hour geeks() is called.
schedule.every(TICK).minutes.do(build_the_bridge)
# run first time
build_the_bridge()

# Loop so that the scheduling task
# keeps on running all time.
while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    time.sleep(1)

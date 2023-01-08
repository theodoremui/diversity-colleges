########################################################################
# prewarm.py
#
# pre-warm the site by pre-fetching pages from servers
########################################################################

import sys
import ourrequests
import asyncio
from concurrent.futures import ProcessPoolExecutor

OUTPUT_DIR="data"
SCHOOL="berkeley"
SUBJECT="opinion"

LISTING_BASE_URL = f"https://dailycal.org/section/{SUBJECT}/page/"

def printUsage(progname):
    print("Usage: python {} <startPage> <endPage>".format(
        progname.split('/')[-1]
    ))

def prewarm(startPage, endPage):
    try:
        print(f"Prewarming {SCHOOL} {SUBJECT} from {startPage} to {endPage}")
        for page in range(startPage, endPage+1):
            url = LISTING_BASE_URL + str(page)
            print(f"-> {url}")
            asyncio.run(ourrequests.asyncGet(url))
    except Exception as e:
        print(f"Exception: {e}")

#=====================================================================
# main
#=====================================================================
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print(len(sys.argv))
        printUsage(sys.argv[0]) 
        sys.exit(0)

    startPage  = int(sys.argv[1])
    endPage    = int(sys.argv[2])

    prewarm(startPage, endPage)
    print("DONE")

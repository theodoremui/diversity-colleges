# Package: 'unbuffer'
#     - may need to install: "brew install expect" for unbuffer
# 
# Package: 'watch'
#     - may need to install: "brew install watch"
#     - to watch growth of log:
#           `watch -n 5 "ls -la ./berkeley-opinion-LATEST.log"`
#
unbuffer python3 ./src/unc2parquet.py 1 467 3 2>&1 | tee -a ./data/unc-opinion-LATEST.log

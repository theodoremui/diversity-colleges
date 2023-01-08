# Package: 'unbuffer'
#     - may need to install: "brew install expect" for unbuffer
# 
# Package: 'watch'
#     - may need to install: "brew install watch"
#     - to watch growth of log:
#           `watch -n 5 "ls -la ./berkeley-opinion-LATEST.log"`
#

unbuffer python ./src/stanford2parquet-sd.py 2>&1 | tee ./data/stanford-opinion-LATEST.log

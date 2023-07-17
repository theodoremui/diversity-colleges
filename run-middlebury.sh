# Package: 'unbuffer'
#     - may need to install: "brew install expect" for unbuffer
# 
# Package: 'watch'
#     - may need to install: "brew install watch"
#     - to watch growth of log:
#           `watch -n 5 "ls -la ./brown-opinion-LATEST.log"`
#
# unbuffer python ./src/middlebury2parquet.py 1 5 3 2>&1 | tee -a ./data/middlebury-opinion-LATEST.log
unbuffer python ./src/middlebury2parquet.py 7 151 3 2>&1 | tee -a ./data/middlebury-opinion-LATEST.log
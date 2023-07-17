# Package: 'unbuffer'
#     - may need to install: "brew install expect" for unbuffer
# 
# Package: 'watch'
#     - may need to install: "brew install watch"
#     - to watch growth of log:
#           `watch -n 5 "ls -la ./brown-opinion-LATEST.log"`
#
unbuffer python ./src/brown2parquet.py 1 224 3 2>&1 | tee -a ./data/brown-opinion-LATEST.log
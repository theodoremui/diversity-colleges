# Package: 'unbuffer'
#     - may need to install: "brew install expect" for unbuffer
# 
# Package: 'watch'
#     - may need to install: "brew install watch"
#     - to watch growth of log:
#           `watch -n 5 "ls -la ./harvard-opinion-LATEST.log"`
#
# NOTE that Harvard opinion section has 3 types of articles
#    f"https://www.thecrimson.com/tag/op-eds/page/", # (h1) 1-116, (h2) 117-190
#    f"https://www.thecrimson.com/tag/editorials/page/", # (h1) 1-77, (h2) 78-124
#    f"https://www.thecrimson.com/tag/columns/page/" #1-120 ... 224

unbuffer python ./src/harvard2parquet.py 120 224 1 2>&1 | tee -a ./data/harvard-opinion-LATEST.log

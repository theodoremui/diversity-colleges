#########################################################################
#
# ourrequests.py
#
# @author: Theodore Mui
# @email: theodoremui@gmail.com
# @date: Sat Feb 11 20:01:10 PST 2023
#
#########################################################################

import requests
import os

BIPARTISAN_API_KEY = os.environ.get("BIPARTISAN_API_KEY")

url = "https://api.thebipartisanpress.com/api/endpoints/beta/robert"
payload = {"API": BIPARTISAN_API_KEY, "Text": "trump is bad"}

response = requests.post(url, data=payload)

print(response.text)

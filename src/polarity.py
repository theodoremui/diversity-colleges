import requests
import os

BIPARTISAN_API = os.environ.get("BIPARTISAN_API")

url = "https://api.thebipartisanpress.com/api/endpoints/beta/robert"
data = {"API": BIPARTISAN_API, "Text": "trump is bad"}

response = requests.post(url, data=data)

print(response.text)

import os
import requests

def download_data(pid):
    print(f"Downloading data for project {pid}")
    url = f"https://ashanet.org/project/?pid={pid}"
    response = requests.get(url)
    return response.text

# download and save data for 1 to last project(i.e 1353)
def download_all_data():
    i = 1
    while True:
        data = download_data(i)
        # convert data to html and save it
        if not os.path.exists("Download/HTML_DATA"):
            os.makedirs("Download/HTML_DATA")
        with open(f"Download/HTML_DATA/ashasup_{i}.html", "w") as f:
            f.write(data)
        i += 1
            


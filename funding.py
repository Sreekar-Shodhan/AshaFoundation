import requests

def download_data(pid):
    url = f"https://ashanet.org/project/?pid={pid}"
    response = requests.get(url)
    return response.text

# download and save data for 1 to last project(i.e 1353)
for i in range(1, 1353):
    data = download_data(i)
    # convert data to html and save it
    with open(f"ashasup_{i}.html", "w") as f:
        f.write(data)
        



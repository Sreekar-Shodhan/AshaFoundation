import re

big_string = "Dec 2002St. LouisUSD 1500Dec 2000St. LouisINR 1500Dec 2004Silicon ValleyUSD 1500Dec 2000ArizonaINR 1500"
parts = re.findall(r'(.*?(?:USD|INR)\s+\d+)', big_string)
print(parts)


pattern = r'(\w+)\s+(\d+)([A-Za-z\. ]+)(USD|INR)\s+(\d+)'

for string in parts:
    match = re.match(pattern, string)
    print(match)
    if match:
        month = match.group(1)
        year = match.group(2)
        area = match.group(3)
        currency = match.group(4)
        amount = match.group(5)
    

FROM python:3.9 

WORKDIR /AshaFoundation

COPY requirements.txt . 

RUN pip install -r requirements.txt

COPY . .

#use command docker run -it python-ashafoundation extract_yc 
#extract_yc is a command in cli.py which creates yearcurr.csv 
CMD ["python", "cli.py"] 


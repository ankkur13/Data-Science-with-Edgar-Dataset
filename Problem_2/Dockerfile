# Use the basic Python 3 image as launching point
FROM python:3.6.3

# Add the script or text to the Dockerfile
ADD eda_log.py /home
ADD requirements.txt /home

# Install required Libraries
RUN pip install -r ./home/requirements.txt
RUN pip install lxml
RUN pip install numpy
RUN pip install pandas
RUN pip install requests
RUN pip install seaborn
# Use the basic Python 3 image as launching point
FROM python:3.6.3

# Add the script or text to the Dockerfile
ADD extract_tables.py /home
ADD requirements.txt /home
ADD argv_input_syntax.txt /home

# Install required Libraries
RUN pip install -r ./home/requirements.txt
FROM python:3.10
ADD client_ref.py .
ADD requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "./client_ref.py", "http://challenge2025.debs.org:52923"] 
#remove /api from end because of example code structure

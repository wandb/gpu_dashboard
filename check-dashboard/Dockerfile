FROM --platform=linux/amd64 python:3.11
WORKDIR /check-dashboard
COPY . /check-dashboard
RUN pip3 install -r requirements.txt
CMD ["python3", "check_dashboard.py"]
FROM --platform=linux/amd64 python:3.11
WORKDIR /gpu-dashboard
COPY . /gpu-dashboard
RUN pip3 install -r requirements.txt
CMD ["python3", "GpuUsage.py"]
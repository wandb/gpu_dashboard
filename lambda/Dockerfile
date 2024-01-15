# FROM public.ecr.aws/lambda/python:3.9
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.11
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"
COPY . ${LAMBDA_TASK_ROOT}

CMD [ "foo.handler" ]

# RUN pip install -r requirements.txt "${LAMBDA_TASK_ROOT}"
# RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}" && yum install -y npm

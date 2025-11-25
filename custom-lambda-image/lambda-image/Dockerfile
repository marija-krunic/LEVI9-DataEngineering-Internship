FROM public.ecr.aws/lambda/python:3.11

# Install extra libs inside image if needed (example only):
# RUN pip install pandas

COPY app.py ${LAMBDA_TASK_ROOT}

CMD ["app.lambda_handler"]

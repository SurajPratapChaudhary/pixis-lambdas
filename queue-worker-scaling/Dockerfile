# Use the official AWS Lambda Python image for Python 3.9
FROM public.ecr.aws/lambda/python:3.9

# Set the working directory in the container
WORKDIR ${LAMBDA_TASK_ROOT}
# Copy the requirements file into the container
COPY ./queue-worker-scaling/requirements.txt .

# Install dependencies
#RUN pip install -r requirements.txt
RUN pip install -r requirements.txt

# Copy function code
COPY ./queue-worker-scaling/ .
# Set the CMD to your handler
CMD [ "QueueWorkerScaling.lambda_handler" ]


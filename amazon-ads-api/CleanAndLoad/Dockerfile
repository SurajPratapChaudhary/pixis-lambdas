# Use the official AWS Lambda Python image for Python 3.9
FROM public.ecr.aws/lambda/python:3.8

# Set the working directory in the container
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy the requirements file into the container
COPY ./amazon-ads-api/CleanAndLoad/requirements.txt .

# Install dependencies
#RUN pip install -r requirements.txt
RUN pip install -r requirements.txt

# Copy function code
COPY ./amazon-ads-api/CleanAndLoad/ .
COPY ./amazon-ads-api/includes ./includes
RUN touch bq-credentials-peakroas.json

# Set the CMD to your handler
CMD [ "Lambda-CleanAndLoad.lambda_handler" ]


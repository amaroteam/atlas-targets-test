# Define function directory
ARG FUNCTION_DIR="/function"

FROM python:3.8-buster

# Install aws-lambda-cpp build dependencies
RUN apt-get update && \
  apt-get install -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev \
  git

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Create function directory
RUN mkdir -p ${FUNCTION_DIR}
RUN mkdir  ${FUNCTION_DIR}/task_classes
RUN mkdir  ${FUNCTION_DIR}/citadel

COPY main.py  ${FUNCTION_DIR}
COPY requirements.txt  ${FUNCTION_DIR}
COPY task_classes/ ${FUNCTION_DIR}/task_classes/
COPY citadel/ ${FUNCTION_DIR}/citadel/
COPY --from=public.ecr.aws/datadog/lambda-extension:latest /opt/extensions/ /opt/extensions

RUN pip install -r ${FUNCTION_DIR}/requirements.txt --target ${FUNCTION_DIR}

# Install the runtime interface client
RUN pip install \
        --target ${FUNCTION_DIR} \
        awslambdaric

WORKDIR ${FUNCTION_DIR}

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "datadog_lambda.handler.handler" ]
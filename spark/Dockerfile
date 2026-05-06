FROM bitnamilegacy/spark:3.5.1

USER root
RUN python3 -m pip install --no-cache-dir \
    numpy==1.26.4 \
    pandas==2.2.2 \
    joblib==1.4.2 \
    scikit-learn==1.5.0
RUN mkdir -p /tmp/checkpoint/secom-streaming && chown -R 1001:1001 /tmp/checkpoint
USER 1001

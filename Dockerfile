FROM continuumio/anaconda3
WORKDIR /app
COPY main2.py /app/script.py

# Create environment and install packages
RUN conda create -n myenv python=3.9 -y && \
    conda install -n myenv pandas=1.3.3 matplotlib=3.4.3 seaborn=0.11.2 numpy tqdm -y && \
    /opt/conda/envs/myenv/bin/pip install \
        google-cloud-bigquery \
        "pandas-gbq==0.17.0" \
        markovclick \
        marketing_attribution_models \
        networkx \
        graphviz && \
    conda clean --all -y

# Set the PATH to prioritize myenv's binaries
ENV PATH="/opt/conda/envs/myenv/bin:$PATH"

# Use the environment's Python directly
CMD ["python", "/app/script.py"]
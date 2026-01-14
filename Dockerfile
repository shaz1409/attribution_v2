# FROM continuumio/anaconda3
# WORKDIR /app
# COPY main2.py /app/script.py

# # Create environment and install packages
# RUN conda create -n myenv python=3.9 -y && \
#     conda install -n myenv pandas=1.3.3 matplotlib=3.4.3 seaborn=0.11.2 numpy tqdm -y && \
#     /opt/conda/envs/myenv/bin/pip install \
#         google-cloud-bigquery \
#         "pandas-gbq==0.17.0" \
#         markovclick \
#         marketing_attribution_models \
#         networkx \
#         graphviz && \
#     conda clean --all -y

# # Set the PATH to prioritize myenv's binaries
# ENV PATH="/opt/conda/envs/myenv/bin:$PATH"

# # Use the environment's Python directly
# CMD ["python", "/app/script.py"]


FROM continuumio/anaconda3
WORKDIR /app

# Copy script (kept as /app/script.py since your CMD uses it)
COPY main2.py /app/script.py

# System deps for Graphviz rendering (binary `dot`)
RUN apt-get update && apt-get install -y graphviz && rm -rf /var/lib/apt/lists/*

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

# Use myenv binaries first
ENV PATH="/opt/conda/envs/myenv/bin:$PATH"

# #2 Unbuffered logs; #3 Headless matplotlib backend
ENV PYTHONUNBUFFERED=1
ENV MPLBACKEND=Agg

# Unbuffered Python runner (also -u for belt & braces)
CMD ["python", "-u", "/app/script.py"]

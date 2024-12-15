# Use uma imagem base com Python
FROM python:3.9-slim

# Atualize e instale dependências essenciais
RUN apt-get update && apt-get install -y \
    libmysqlclient-dev \
    && apt-get clean

# Defina o diretório de trabalho
WORKDIR /app

# Copie os arquivos do projeto para o container
COPY . /app

# Instale dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Defina o comando padrão
CMD ["python", "etl_script.py"]
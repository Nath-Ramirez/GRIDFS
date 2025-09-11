# Imagen base ligera de Python
FROM python:3.11-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar requirements primero (mejor cacheo de capas)
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código del NameNode
COPY . .

# Exponer el puerto del NameNode
EXPOSE 5000

# Comando de ejecución
CMD ["python", "app.py"]

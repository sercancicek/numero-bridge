# mssql-python-pyodbc
# Python runtime with pyodbc to connect to SQL Server
FROM laudio/pyodbc

RUN mkdir /code
WORKDIR /code

RUN pip install pipenv
COPY ./Pipfile* /code/
RUN cd /code && pipenv lock --requirements && pipenv lock --requirements > requirements.txt
RUN pip install -r requirements.txt

COPY . /code/

CMD [ "python", "./main.py" ]

# echo "daphne -b 0.0.0.0 -p 8000 backend.asgi:application" >> /code/start_api.sh && \
import pandas as pd
import numpy as np
import psycopg2
from datetime import date
from random import randint
from typing import Union


class XlsxReader:
    r'''
    Reads file as pandas DataFrame
    '''
    def __init__(self, path : str) -> None:
        self.dataframe = pd.read_excel(path, header=[0, 1, 2], index_col=0)
        d = []
        for _ in range(len(self.dataframe['fact'])):
            d.append(date(year=2023, month=1, day=randint(1, 5)))
        # adding random dates column in datetime.date format.
        self.dataframe['Date'] = d


class XlsxToDatabase:
    r'''
    Class to save your parsed data to PostgreSQL database. To create an instance you need to set:
    
    database_user: string, username to connect
    database_password: string, password of user
    database_adress: string, host where database stored
    database_name: string, name of database
    database_port: int, optional, if your database not on default 5432 port
    '''
    def __init__(self,
                database_user: str,
                database_password: str,
                database_adress: str,
                database_name: str,
                database_port = 5432) -> None:

        # Saving database connection properties
        self.db_username = database_user
        self.db_password = database_password
        self.db_host = database_adress
        self.db_port = database_port
        self.db_name = database_name
        
        # Table initiation
        try:
            connection = psycopg2.connect(database=self.db_name,
                                          user=self.db_username,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port)
        except:
            print('[ERROR] Can`t establish connection to database')
        else:
            cursor = connection.cursor()
            print("Информация о сервере PostgreSQL\n\n")
            print(connection.get_dsn_parameters(), "\n")
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
            print("Вы подключены к - ", record, "\n")
            cursor.execute('''
CREATE TABLE IF NOT EXISTS Company(
                    id SERIAL PRIMARY KEY,
                    name varchar(100)
                );

CREATE TABLE IF NOT EXISTS Measurement_Type(
                    id SERIAL PRIMARY KEY,
                    type varchar(100)
                );

CREATE TABLE IF NOT EXISTS Measurement(
                    id SERIAL PRIMARY KEY,
                    company_id int,
                    measurement_type_id int,
                    liq_1 int,
                    liq_2 int,
                    oil_1 int,
                    oil_2 int,
                    date date,
                    CONSTRAINT fk_forecast
                        FOREIGN KEY(company_id)
                        REFERENCES company(id),
                        FOREIGN KEY(measurement_type_id)
                        REFERENCES Measurement_Type(id)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE
                );
            ''')
            cursor.execute('''
INSERT INTO Measurement_type(type) VALUES ('fact') ON CONFLICT (name) DO NOTHING;
INSERT INTO Measurement_type(type) VALUSE ('forecast') ON CONFLICT (type) DO NOTHING;
            ''')
            cursor.close()
            connection.commit()
            connection.close()

    def save(self, data: XlsxReader) -> None:
        dataframe = data.dataframe
        for row in dataframe.iloc:
            # getting row from 
            data_list = row.to_list()
            connection = psycopg2.connect(database=self.db_name,
                                          user=self.db_username,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port)
            cursor = connection.cursor()
            # getting company_id if new company, add and then get id
            try:
                cursor.execute('INSERT INTO Company(name) VALUES (%s);', [data_list[0]])
                connection.commit()
            except:
                print('[WARNING] company already exists')
                connection.rollback()
            finally:
                cursor.execute('SELECT id FROM company WHERE name=%s', [data_list[0]])
                company_id = cursor.fetchone()
            # adding fact measurements
            try:
                args_list = list(company_id)+data_list[1:5]+[data_list[-1]]
                args_list = [elem.item() if type(elem)==np.int64 else elem for elem in args_list]
                cursor.execute('''
                INSERT INTO Measurement(company_id, measurement_type_id, liq_1, liq_2, oil_1, oil_2, date)
                    VALUES (%s, 1, %s, %s, %s, %s, %s);
                ''', args_list)
            except:
                connection.rollback()
                print('[ERROR] cant insert into forecast measurements on %s iteration' % id)

            # adding forecast measurements
            try:
                args_list = list(company_id)+data_list[5:]
                # ints from excel reads as numpy.int64 - convert them to vanilla Python int, cause psqcorg2 can't use numpy.int64
                args_list = [elem.item() if type(elem)==np.int64 else elem for elem in args_list]
                cursor.execute('''
                    INSERT INTO Measurement(company_id, measurement_type_id, liq_1, liq_2, oil_1, oil_2, date)
                        VALUES (%s, 2, %s, %s, %s, %s, %s);
                ''', args_list)
            except:
                connection.rollback()
                print('[ERROR] cant insert into forecast measurements on %s iteration' % id)
            connection.commit()
            cursor.close()
            connection.close()


class EstimatedTotalByData:
    
    data = {}

    def __init__(self, database_user: str,
                database_password: str,
                database_adress: str,
                database_name: str,
                database_port = 5432) -> None:
        self.db_username = database_user
        self.db_password = database_password
        self.db_host = database_adress
        self.db_port = database_port
        self.db_name = database_name

        try:
            connection = psycopg2.connect(database=self.db_name,
                                        user=self.db_username,
                                        password=self.db_password,
                                        host=self.db_host,
                                        port=self.db_port)
        except:
            print('[ERROR] Can`t establish connection to database')
    
    def get_qliq_fact_estimated_total(self, company_id: int) -> Union[pd.DataFrame, str]:
        try:
            connection = psycopg2.connect(database=self.db_name,
                                          user=self.db_username,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port)
        except:
            return '[ERROR] Can`t establish connection to database'
        try:
            cursor = connection.cursor()
            cursor.execute('''
            WITH company AS
            (
            	SELECT * FROM measurement
            	WHERE company_id=%s
            )
            SELECT date, SUM(liq_1)+SUM(liq_2) as est_total
            FROM company
            WHERE measurement_type_id = 1
            GROUP BY date
            ORDER BY date;
            ''', [company_id])
            record = cursor.fetchall()
        except:
            return "[ERROR] Can't get estimated total for fact Qliq value"
        else:
            return pd.DataFrame(record, columns=['Date', 'Estimated_Total'])

    def get_qliq_forecast_estimated_total(self, company_id: int) -> Union[pd.DataFrame, str]:
        try:
            connection = psycopg2.connect(database=self.db_name,
                                          user=self.db_username,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port)
        except:
            return '[ERROR] Can`t establish connection to database'
        try:
            cursor = connection.cursor()
            cursor.execute('''
                    WITH company AS
                    (
                    	SELECT * FROM measurement
                    	WHERE company_id=%s
                    )
                    SELECT date, SUM(liq_1)+SUM(liq_2) as est_total
                    FROM company
                    WHERE measurement_type_id = 2
                    GROUP BY date
                    ORDER BY date;
                    ''', [company_id])
            record = cursor.fetchall()
        except:
            return "[ERROR] Can't get estimated total for forecast Qliq value"
        else:
            return pd.DataFrame(record, columns=['Date', 'Estimated_Total'])

    def get_qoil_fact_estimated_total(self, company_id: int) -> Union[pd.DataFrame, str]:
        try:
            connection = psycopg2.connect(database=self.db_name,
                                          user=self.db_username,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port)
        except:
            return '[ERROR] Can`t establish connection to database'
        try:
            cursor = connection.cursor()
            cursor.execute('''
                    WITH company AS
                    (
                    	SELECT * FROM measurement
                    	WHERE company_id=%s
                    )
                    SELECT date, SUM(oil_1)+SUM(oil_2) as est_total
                    FROM company
                    WHERE measurement_type_id = 1
                    GROUP BY date
                    ORDER BY date;
                    ''', [company_id])
            record = cursor.fetchall()
        except:
            return "[ERROR] Can't get estimated fact for forecast Qliq value"
        else:
            return pd.DataFrame(record, columns=['Date', 'Estimated_Total'])

    def get_qoil_forecast_estimated_total(self, company_id: int) -> Union[pd.DataFrame, str]:
        try:
            connection = psycopg2.connect(database=self.db_name,
                                          user=self.db_username,
                                          password=self.db_password,
                                          host=self.db_host,
                                          port=self.db_port)
        except:
            return '[ERROR] Can`t establish connection to database'
        try:
            cursor = connection.cursor()
            cursor.execute('''
                    WITH company AS
                    (
                    	SELECT * FROM measurement
                    	WHERE company_id=%s
                    )
                    SELECT date, SUM(oil_1)+SUM(oil_2) as est_total
                    FROM company
                    WHERE measurement_type_id = 2
                    GROUP BY date
                    ORDER BY date;
                    ''', [company_id])
            record = cursor.fetchall()
        except:
            return "[ERROR] Can't get estimated fact for forecast Qliq value"
        else:
            return pd.DataFrame(record, columns=['Date', 'Estimated_Total'])
    
    def get_all(self) -> Union[dict, str]:
            try:
                connection = psycopg2.connect(database=self.db_name,
                                              user=self.db_username,
                                              password=self.db_password,
                                              host=self.db_host,
                                              port=self.db_port)
            except:
                return '[ERROR] Can`t establish connection to database'

            cursor = connection.cursor()
            try:
                cursor.execute('''
                SELECT name FROM company;
                ''')
            except:
                connection.rollback()
                return "[ERROR] Can't find any companies"
            else:
                companies = [elem[0] for elem in cursor.fetchall()]
            print(companies)
            for company in companies:
                cursor.execute('SELECT id FROM company WHERE name=%s', [company])
                company_id = cursor.fetchone()[0]
                self.data[company] = {
                    'fact' : {
                                'Qliq' : self.get_qliq_fact_estimated_total(company_id=company_id),
                                'Qoil' : self.get_qoil_fact_estimated_total(company_id=company_id)
                             },
                    'forecast' : {
                                'Qliq' : self.get_qliq_forecast_estimated_total(company_id=company_id),
                                'Qoil' : self.get_qoil_forecast_estimated_total(company_id=company_id)
                    }
                }
            return self.data

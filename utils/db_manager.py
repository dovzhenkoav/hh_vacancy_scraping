from utils import config
import psycopg2


class DBManager:
    db_config = config()
    database_name = 'hh_data'

    @classmethod
    def create_database(cls):
        conn = psycopg2.connect(dbname='postgres', **cls.db_config)
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(f"DROP DATABASE  {cls.database_name}")
        except psycopg2.errors.InvalidCatalogName:
            cur.execute(f"CREATE DATABASE  {cls.database_name}")
        else:
            cur.execute(f"CREATE DATABASE  {cls.database_name}")

        cur.close()
        conn.close()

        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE employers
                (
                    employer_id int PRIMARY KEY,
                    employer_name VARCHAR(55) NOT NULL,
                    site_url TEXT,
                    hh_url TEXT,
                    town VARCHAR(55),
                    trusted BOOLEAN,
                    description TEXT
                )
                """)

        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE vacancies
                (
                    vacancy_id SERIAL PRIMARY KEY,
                    vacancy_name VARCHAR(255) NOT NULL,
                    employer_id INT REFERENCES employers(employer_id),
                    url TEXT,
                    address VARCHAR(255),
                    archived BOOLEAN,
                    created_at TIMESTAMP,
                    employment VARCHAR(55),
                    requirement TEXT,
                    responsibility TEXT,
                    salary_currency VARCHAR(55),
                    salary_from DECIMAL,
                    salary_to DECIMAL
                )
                """)

        conn.commit()
        conn.close()

    @classmethod
    def insert_data(cls, data: list[dict]):
        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        try:
            for employer_vacancies in data:
                employer = employer_vacancies['employer']
                vacancies = employer_vacancies['vacancies']

                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO employers(employer_id, employer_name, description, site_url, hh_url, town, trusted)
                        VALUES(%s, %s, %s, %s, %s, %s, %s)
                        """, (
                        employer['id'], employer['name'], employer['description'], employer['site_url'],
                        employer['hh_url'],
                        employer['town'], employer['trusted']))
                if vacancies:
                    for vacancy in vacancies:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO vacancies(vacancy_name, employer_id, url, address, archived, created_at, employment, requirement, responsibility, salary_currency, salary_from, salary_to)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """, (
                                vacancy['name'], employer['id'], vacancy['url'], vacancy['address'],
                                vacancy['archived'],
                                vacancy['created_at'], vacancy['employment'], vacancy['requirement'],
                                vacancy['responsibility'], vacancy['salary_currency'], vacancy['salary_from'],
                                vacancy['salary_to']))

        except Exception as err:
            raise err
        finally:
            conn.commit()
            conn.close()

    @classmethod
    def get_companies_and_vacancies_count(cls) -> list:
        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT employer_name, COUNT(*) FROM vacancies
                    RIGHT JOIN employers USING(employer_id)
                    GROUP BY employer_name
                """)
                results = cur.fetchall()
        except Exception as err:
            raise err
        finally:
            conn.commit()
            conn.close()
        return results

    @classmethod
    def get_all_vacancies(cls) -> list:
        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT employer_name, vacancy_name, url, CONCAT(salary_from, '-', salary_to, ' ', salary_currency) as salary 
                    FROM vacancies
                    JOIN employers USING(employer_id)
                """)
                results = cur.fetchall()
        except Exception as err:
            raise err
        finally:
            conn.commit()
            conn.close()
        return results

    @classmethod
    def get_avg_salary(cls) -> str:
        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT AVG(salary_from) as avg_salary
                    FROM vacancies
                    JOIN employers USING(employer_id)
                """)
                results = cur.fetchone()
        except Exception as err:
            raise err
        finally:
            conn.commit()
            conn.close()
        return f'Средняя начальная ЗП: {int(results[0])} рублей'

    @classmethod
    def get_vacancies_with_higher_salary(cls) -> list:
        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM vacancies WHERE salary_to > (
                    SELECT AVG(salary_from)
                    FROM vacancies
                    JOIN employers USING(employer_id)
                    )
                """)
                results = cur.fetchall()
        except Exception as err:
            raise err
        finally:
            conn.commit()
            conn.close()
        return results

    @classmethod
    def get_vacancies_with_keyword(cls, keyword: str) -> list:
        conn = psycopg2.connect(dbname=cls.database_name, **cls.db_config)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM vacancies
                    WHERE vacancy_name like %s
                """, (f"%{keyword}%)",))
                results = cur.fetchall()
        except Exception as err:
            raise err
        finally:
            conn.commit()
            conn.close()
        return results

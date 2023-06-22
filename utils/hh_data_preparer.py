import re


class HHDataPreparer:

    def __new__(cls, data: list[dict]):
        pure_data = []

        for employer_and_vacancies in data:
            employer = employer_and_vacancies['employer']
            vacancies = employer_and_vacancies['vacancies']

            pure_employer = {
                'id': int(employer['id']),
                'name': employer['name'],
                'site_url': employer['site_url'],
                'hh_url': employer['alternate_url'],
                'trusted': employer['trusted'],
                'description': re.sub(r'(\<(/?[^>]+)>)', '', employer['description']),
                'town': employer['area']['name']
            }
            pure_vacancies = []
            for vacancy in vacancies:
                pure_vacancies.append({
                    'id': vacancy['id'],
                    'name': vacancy['name'],
                    'address': cls._get_address(vacancy['address']),
                    'url': vacancy['alternate_url'],
                    'archived': vacancy['archived'],
                    'created_at': vacancy['created_at'],
                    'employment': vacancy['employment']['name'],
                    'salary_from': cls._get_salary_from(vacancy['salary']),
                    'salary_to': cls._get_salary_to(vacancy['salary']),
                    'salary_currency': cls._get_salary_currency(vacancy['salary']),
                    'requirement': vacancy['snippet']['requirement'],
                    'responsibility': vacancy['snippet']['responsibility']
                })
            pure_data.append({'employer': pure_employer, 'vacancies': pure_vacancies})
        return pure_data

    @classmethod
    def _get_address(cls, address):
        if address is None:
            return None
        else:
            return address['raw']

    @classmethod
    def _get_salary_from(cls, salary_from):
        if salary_from is None:
            return None
        else:
            return salary_from['from']

    @classmethod
    def _get_salary_to(cls, salary_to):
        if salary_to is None:
            return None
        else:
            return salary_to['to']

    @classmethod
    def _get_salary_currency(cls, salary_currency):
        if salary_currency is None:
            return None
        else:
            return salary_currency['currency']

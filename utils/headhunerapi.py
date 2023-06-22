import requests


class HeadHunterAPI:
    """API class, that allows to get data from hh.ru."""
    def __init__(self):
        self._HH_EMPLOYERS_ENDPOINT = "https://api.hh.ru/employers/"
        self.max_results_per_page = 100
        self.only_with_vacancies = True

    def get_all_employers_vacancies(self, employers: dict[str, int]) -> list[dict]:
        """Get vacancies from hh.ru."""
        all_employers_vacancies = []
        for employer_id in employers.values():
            employer_data: dict[str] = self._get_employer_data(employer_id)
            employer_vacancies_data: list[dict] = self._get_employer_vacancies_data(employer_data)
            parsed_employer_and_vacancies_data = self._parse_employer_and_vacancies_data(employer_data,
                                                                                         employer_vacancies_data)
            all_employers_vacancies.append(parsed_employer_and_vacancies_data)

        return all_employers_vacancies

    def _parse_employer_and_vacancies_data(self, employer_data: dict[str], employer_vacancies_data: list[dict]):
        return {'employer': employer_data,
                'vacancies': employer_vacancies_data}

    def _get_employer_vacancies_data(self, employer_data: dict[str]) -> list[dict]:
        vacancies_url = employer_data['vacancies_url']
        all_employers_vacancies = []
        page = 0

        for _ in range(10):
            params = {
                "per_page": self.max_results_per_page,
                "page": page
            }

            response = requests.get(vacancies_url)
            all_employers_vacancies.extend(response.json()['items'])
            if page >= response.json()['pages']:
                break
            else:
                page += 1

        return all_employers_vacancies

    def _get_employer_data(self, employer_id: int) -> dict[str]:
        params = {
            "only_with_vacancies": self.only_with_vacancies,
            "per_page": self.max_results_per_page,
        }

        response = requests.get(f'{self._HH_EMPLOYERS_ENDPOINT}{employer_id}', params=params)
        return response.json()

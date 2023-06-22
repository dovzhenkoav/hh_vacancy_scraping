from pprint import pprint

from utils import HeadHunterAPI, HHDataPreparer, DBManager

employers_id = {
    "Appfox": 3749373,
    "Такском": 42080,
    "Haulmont": 246739,
    "Иннотех": 4649269,
    "CATAPULTO.RU": 1776381,
    "ТОО SOMNIUM": 2522589,
    "Grass": 1073798,
    "ООО УайтСнейк": 5674346,
    "BRANDPOL": 3672566,
    "ООО Точка Зрения": 4485384
}


def main():
    """Main function."""
    headhunter_api = HeadHunterAPI()

    hh_data = headhunter_api.get_all_employers_vacancies(employers_id)
    clean_data = HHDataPreparer(hh_data)

    DBManager.create_database()
    DBManager.insert_data(clean_data)

    pprint(DBManager.get_companies_and_vacancies_count())
    pprint(DBManager.get_all_vacancies())
    pprint(DBManager.get_avg_salary())
    pprint(DBManager.get_vacancies_with_higher_salary())
    pprint(DBManager.get_vacancies_with_keyword('Python'))


if __name__ == '__main__':
    main()

import time
from typing import List, Dict, Union
from datetime import datetime
import pandas as pd
from tinydb import TinyDB
from selenium import webdriver
from selenium.webdriver.common.by import By


class SuperjobScraper:
    def __init__(self, driver_path: str):
        self.driver = webdriver.Chrome()
        self.base_url = "https://www.superjob.ru"

    def scrape_vacancies(self, position: str, city: str) -> List[Dict[str, Union[str, int, None]]]:
        vacancies = []
        try:
            self.driver.get(f"{self.base_url}/vacancy/search/?keywords={position}&geo[c][0]=1&geo[t][0]=4")
            time.sleep(50)  

            while True:
                vacancy_elements = self.driver.find_elements(By.CLASS_NAME, "_3mfro")
                for vacancy_element in vacancy_elements:
                    vacancy = self.parse_vacancy_element(vacancy_element)
                    if vacancy:
                        vacancies.append(vacancy)

                next_button = self.driver.find_element(By.CLASS_NAME, "icMQ_")
                if not next_button or "disabled" in next_button.get_attribute("class"):
                    break
                next_button.click()
                time.sleep(5)  

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            self.driver.quit()

        return vacancies

    def parse_vacancy_element(self, element) -> Dict[str, Union[str, int, None]]:
        try:
            position = element.find_element(By.CLASS_NAME, "_1QIBo").text
            salary_text = element.find_element(By.CLASS_NAME, "_2Wp8I").text
            salary_from, salary_to = self.parse_salary(salary_text)
            currency = salary_text.split()[-1]
            published_at = element.find_element(By.CLASS_NAME, "_3MVeX").text

            vacancy = {
                "position": position,
                "created_at": datetime.utcnow().isoformat() + "Z",
                "published_at": published_at,
                "salary_from": salary_from,
                "salary_to": salary_to,
                "currency": currency,
                "city": "Москва",  
                "description": "", 
                "link": element.find_element(By.TAG_NAME, "a").get_attribute("href")
            }
            return vacancy
        except Exception as e:
            print(f"An error occurred while parsing vacancy: {e}")
            return None

    def parse_salary(self, salary_text: str) -> Union[int, None]:
        if "—" in salary_text:
            salary_from, salary_to = map(lambda x: int(x.replace("\xa0", "").replace("руб.", "").strip()),
                                         salary_text.split("—"))
            return salary_from, salary_to
        elif "от" in salary_text:
            salary_from = int(salary_text.replace("\xa0", "").replace("руб.", "").replace("от", "").strip())
            return salary_from, None
        elif "до" in salary_text:
            salary_to = int(salary_text.replace("\xa0", "").replace("руб.", "").replace("до", "").strip())
            return None, salary_to
        else:
            return None, None


class DataProcessor:
    @staticmethod
    def process_vacancies(vacancies: List[Dict[str, Union[str, int, None]]]) -> pd.DataFrame:
        df = pd.DataFrame(vacancies)
        
        return df


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db = TinyDB(db_path)

    def save_vacancies(self, vacancies: List[Dict[str, Union[str, int, None]]]):
        for vacancy in vacancies:
            self.db.insert(vacancy)

    def read_vacancies(self) -> List[Dict[str, Union[str, int, None]]]:
        return self.db.all()


def main():
   
    driver_path = "chromedriver.exe"
    
    db_path = "vacancies.json"

   
    scraper = SuperjobScraper(driver_path)
   
    vacancies = scraper.scrape_vacancies("садовник", "москва")  

    
    processed_data = DataProcessor.process_vacancies(vacancies)

   
    db_manager = DatabaseManager(db_path)
    db_manager.save_vacancies(vacancies)

   
    saved_vacancies = db_manager.read_vacancies()
    print(saved_vacancies)


if __name__ == "__main__":
    main()

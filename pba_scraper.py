from lxml import etree
import csv
import requests
import threading


def save_records_to_csv(records: list, fields: list, filename: str):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


class PBATeamScraper:
    TEAM_HEAD_COACH = "Head Coach"
    TEAM_LOGO = "Logo Link"
    TEAM_MANAGER = "Manger"
    TEAM_NAME = "Team Name"
    TEAM_URL = "Url"
    CSV_FIELDS = [
        TEAM_HEAD_COACH,
        TEAM_LOGO,
        TEAM_MANAGER,
        TEAM_NAME,
        TEAM_URL,
    ]
    TEAM_LIST_URL = "https://www.pba.ph/teams"

    FILENAME = "team-tester.csv"
    MAX_THREADS = 10

    def __init__(self):
        self.results = []

    def save_to_csv(self):
        save_records_to_csv(self.results, self.CSV_FIELDS, self.FILENAME)

    def scrape(self):
        url_list = self.get_list_of_team_urls()
        url_chunks = [
            url_list[i:i + self.MAX_THREADS]
            for i in range(0, len(url_list), self.MAX_THREADS)
        ]
        for urls in url_chunks:
            self.run_threads(urls)

    def thread_task(self, url):
        self.results.append(self.get_team_data(url))

    def run_threads(self, urls):
        threads = []
        # Create threads
        for url in urls:
            t = threading.Thread(target=self.thread_task, args=(url,))
            threads.append(t)
            t.start()
        # Wait for all threads to complete
        for t in threads:
            t.join()

    def get_list_of_team_urls(self):
        response = requests.get(self.TEAM_LIST_URL)

        if response.status_code != 200:
            raise ValueError('GET TEAM DATA: Invalid Response.')

        # Parse the HTML content.
        tree = etree.HTML(response.text)

        team_url_xpath = '//div[@class="row"]//a[contains(@href, "pba.ph/teams")]/@href'
        url_list = tree.xpath(team_url_xpath)

        return url_list

    def get_team_data(self, team_url: str):
        response = requests.get(team_url)
        tree = etree.HTML(response.text)

        base_xpath = "//div[contains(@class, 'team-personal-bar')]"

        # Team name xpath
        team_name_xpath = base_xpath + "//h3/text()"
        team_name_value = tree.xpath(team_name_xpath)
        team_name_value = team_name_value and team_name_value[0]

        # Head Coach
        head_coach_xpath = base_xpath + "//h5[contains(text(), 'HEAD COACH')]/following-sibling::h5[1]/text()"
        head_coach_value = tree.xpath(head_coach_xpath)
        head_coach_value = head_coach_value and head_coach_value[0]

        # Manager
        manager_xpath = base_xpath + "//h5[contains(text(), 'MANAGER')]/following-sibling::h5/text()"
        manager_value = tree.xpath(manager_xpath)
        manager_value = manager_value and manager_value[0]

        # Url
        url_value = team_url

        # Logo Link
        logo_xpath = base_xpath + "//center//img/@src"
        logo_value = tree.xpath(logo_xpath)
        logo_value = logo_value and logo_value[0]

        return {
            self.TEAM_NAME: team_name_value,
            self.TEAM_HEAD_COACH: head_coach_value,
            self.TEAM_MANAGER: manager_value,
            self.TEAM_URL: url_value,
            self.TEAM_LOGO: logo_value,
        }







def get_teams_data():

    # Team Profile:
    #   > Team name, Head coach, Manager, URL, Logo link

    teams_url = "https://www.pba.ph/teams"

    with open('teams.text', 'w') as f:
        response = requests.get(teams_url)
        f.write(response.text)




def get_players_data():

    # Player Profile:
    #   > Team name, Player name, Number, Position, URL, Mugshot

    players_url = "https://www.pba.ph/players"

    with open('players.text', 'w') as f:
        response = requests.get(players_url)
        f.write(response.text)


if __name__ == "__main__":
    pass
    # get_players_data()
    # get_teams_data()

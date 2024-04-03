from lxml import etree
from pathlib import Path
import csv
import re
import requests
import threading


def clean_text(text_list: list):
    return ' '.join([text.strip() for text in text_list])


def save_records_to_csv(records: list, fields: list, filename: str):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)


def download_image(url, filename):
    default_directory = './media/'        
    path = Path(default_directory)
    path.mkdir(parents=True, exist_ok=True)

    filename = default_directory + filename
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print("Image downloaded successfully as", filename)
        else:
            print("Failed to download image:", response.status_code)
    except Exception as e:
        print("Error:", e)


class BaseClass:
    """
    Base class containing utility methods for scraping PBA (Philippine Basketball Association) data.
    """

    @staticmethod
    def get_team_name(logo_url: str):
        """
        Get the team name from the given logo URL.
        
        Args:
            logo_url (str): URL of the team logo.
        
        Returns:
            str: Team name corresponding to the logo URL.
        """
        return {
            "https://dashboard.pba.ph/assets/logo/Ginebra150.png": "Ginebra San Miguel",
            "https://dashboard.pba.ph/assets/logo/Blackwater_new_logo_2021.png": "Blackwater",
            "https://dashboard.pba.ph/assets/logo/converge-logo2.png": "Converge",
            "https://dashboard.pba.ph/assets/logo/magnolia-2022-logo.png": "Magnolia",
            "https://dashboard.pba.ph/assets/logo/web_mer.png": "Meralco",
            "https://dashboard.pba.ph/assets/logo/web_nlx.png": "NLEX",
            "https://dashboard.pba.ph/assets/logo/GLO_web.png": "North Port",
            "https://dashboard.pba.ph/assets/logo/viber_image_2024-03-05_17-18-02-823.png": "Phoenix",
            "https://dashboard.pba.ph/assets/logo/web_ros.png": "Rain or Shine",
            "https://dashboard.pba.ph/assets/logo/SMB2020_web.png": "San Miguel",
            "https://dashboard.pba.ph/assets/logo/terrafirma.png": "TerraFirma",
            "https://dashboard.pba.ph/assets/logo/tropang_giga_pba.png": "Talk N Text",
        }.get(logo_url)


class PBATeamScraper(BaseClass):
    """
    Class for scraping team information from the PBA (Philippine Basketball Association) website.
    
    Attributes:
        TEAM_HEAD_COACH (str): Field name for the head coach.
        TEAM_LOGO (str): Field name for the logo link.
        TEAM_MANAGER (str): Field name for the manager.
        TEAM_NAME (str): Field name for the team name.
        TEAM_URL (str): Field name for the team URL.
        CSV_FIELDS (list): List containing all field names for CSV.
        TEAM_LIST_URL (str): URL from which team information is scraped.
        FILENAME (str): Name of the CSV file where the scraped data will be saved.
        MAX_THREADS (int): Maximum number of threads to be used for scraping.
    """

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

    FILENAME = "teams.csv"
    MAX_THREADS = 2

    def __init__(self):
        self.results = []

    def save_to_csv(self):
        """
        Save the scraped player information to a CSV file.
        """
        save_records_to_csv(self.results, self.CSV_FIELDS, self.FILENAME)

    def scrape(self):
        """
        Perform scraping of player information from the PBA website.
        """
        url_list = self.get_list_of_team_urls()
        url_chunks = [
            url_list[i:i + self.MAX_THREADS]
            for i in range(0, len(url_list), self.MAX_THREADS)
        ]
        for urls in url_chunks:
            self.run_threads(urls)

    def thread_task(self, url):
        """
        Function executed by each thread to scrape team data from a given URL.
        
        Args:
            url (str): URL of the team to scrape.
        """
        self.results.append(self.get_team_data(url))

    def run_threads(self, urls):
        """
        Run multiple threads to scrape team data from a list of URLs.
        
        Args:
            urls (list): List of URLs to scrape.
        """

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
        """
        Get the list of URLs for all teams from the PBA website.
        
        Returns:
            list: List of team URLs.
        """
        response = requests.get(self.TEAM_LIST_URL)

        if response.status_code != 200:
            raise ValueError('GET TEAM DATA: Invalid Response.')

        # Parse the HTML content.
        tree = etree.HTML(response.text)

        team_url_xpath = '//div[@class="row"]//a[contains(@href, "pba.ph/teams")]/@href'
        url_list = tree.xpath(team_url_xpath)

        return url_list

    def download_image(self, url):
        """
        Download an image from a given URL.
        
        Args:
            url (str): URL of the image to download.
        """

        team_name = self.get_team_name(url)
        filename = None
        if team_name:
            filename =  team_name + '.png'
        if not filename:
            filename = url.split("/")[-1]

        download_image(url, filename)

    def get_team_data(self, team_url: str):
        """
        Get team data from a given team URL.
        
        Args:
            team_url (str): URL of the team to scrape.
        
        Returns:
            dict: Dictionary containing team information.
        """
        # Team Profile:
        #   > Team name, Head coach, Manager, URL, Logo link

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

        # Download and Process Logo 
        if logo_value:
            self.download_image(logo_value)

        return {
            self.TEAM_NAME: team_name_value,
            self.TEAM_HEAD_COACH: head_coach_value,
            self.TEAM_MANAGER: manager_value,
            self.TEAM_URL: url_value,
            self.TEAM_LOGO: logo_value,
        }


class PBAPlayerScraper(BaseClass):
    """
    Class for scraping player information from the PBA (Philippine Basketball Association) website.
    
    Attributes:
        TEAM_NAME (str): Field name for the team name.
        PLAYER_NAME (str): Field name for the player name.
        NUMBER (str): Field name for the player number.
        POSITION (str): Field name for the player position.
        URL (str): Field name for the player URL.
        MUGSHOT (str): Field name for the player mugshot.
        CSV_FIELDS (list): List containing all field names for CSV.
        PLAYERS_URL (str): URL from which player information is scraped.
        FILENAME (str): Name of the CSV file where the scraped data will be saved.
    """

    TEAM_NAME = 'Team name'
    PLAYER_NAME = 'Player name'
    NUMBER = 'Number'
    POSITION = 'Position'
    URL = 'Url'
    MUGSHOT = 'Mugshot'
    CSV_FIELDS = [
        TEAM_NAME,
        PLAYER_NAME,
        NUMBER,
        POSITION,
        URL,
        MUGSHOT,
    ]
    PLAYERS_URL = "https://www.pba.ph/players"
    FILENAME = 'players.csv'

    def __init__(self):
        self.results = []

    def save_to_csv(self):
        """
        Save the scraped player information to a CSV file.
        """
        save_records_to_csv(self.results, self.CSV_FIELDS, self.FILENAME)

    def scrape(self):
        """
        Perform scraping of player information from the PBA website.
        """
        # Player Profile:
        #   > Team name, Player name, Number, Position, URL, Mugshot

        response = requests.get(self.PLAYERS_URL)
        tree = etree.HTML(response.text)

        # Get indivial tree per player
        base_xpath = "//div[@class='playersBox']"
        players_etrees = tree.xpath(base_xpath)

        for p_tree in players_etrees:
            # Team name
            team_name_xpath = "./div[3]//img/@src"
            team_name_value = p_tree.xpath(team_name_xpath)
            team_name_value = team_name_value and team_name_value[0]
            if team_name_value:
                team_name_value = self.get_team_name(team_name_value)

            # Player name
            player_name_xpath = "./div[2]//a[contains(@href, 'players/')]//h5//text()"
            player_name_value = p_tree.xpath(player_name_xpath)
            player_name_value = player_name_value and clean_text(player_name_value)

            # Number
            player_number_xpath = "./div[3]//h6[starts-with(text(), '#')]/text()"
            player_number_value = p_tree.xpath(player_number_xpath)
            player_number_value = player_number_value and player_number_value[0]
            if player_number_value:
                re_value = re.findall(r"^#(\d+)", player_number_value)
                if re_value:
                    player_number_value =  re_value[0]

            # Position
            position_xpath = "./div[3]//h6[starts-with(text(), '#')]/text()"
            position_value = p_tree.xpath(position_xpath)
            position_value = position_value and position_value[0]
            if position_value:
                # get the values from second segment of the text
                position_value = position_value.split('|')[1].strip()

            # url
            url_xpath = "./div[2]//a[contains(@href, 'players/')]/@href"
            url_value = p_tree.xpath(url_xpath)
            url_value = url_value and url_value[0]
            if url_value and not url_value.startswith('/'):
                url_value = 'https://www.pba.ph/' + url_value

            # mugshot
            mugshot_xpath = "./div[1]//a/img/@src"
            mugshot_value = p_tree.xpath(mugshot_xpath)
            mugshot_value = mugshot_value and mugshot_value[0]

            self.results.append(
                {
                    self.TEAM_NAME: team_name_value,
                    self.PLAYER_NAME: player_name_value,
                    self.NUMBER: player_number_value,
                    self.POSITION: position_value,
                    self.URL: url_value,
                    self.MUGSHOT: mugshot_value,
                }
            )


if __name__ == "__main__":

    # Execute Team Scraper
    team_scraper = PBATeamScraper()
    team_scraper.scrape()
    team_scraper.save_to_csv()

    # Execute Player Scraper
    player_scraper = PBAPlayerScraper()
    player_scraper.scrape()
    player_scraper.save_to_csv()


    ### Debugger

    # # DEBUG team scraper
    # PBATeamScraper.FILENAME = 'zz-teams.csv'
    # PBATeamScraper.MAX_THREADS = 10
    # team_scraper = PBATeamScraper()
    # team_scraper.scrape()
    # team_scraper.save_to_csv()

    # # DEBUG player scraper
    # PBAPlayerScraper.FILENAME = 'zz-player.csv'
    # player_scraper = PBAPlayerScraper()
    # player_scraper.scrape()
    # player_scraper.save_to_csv()

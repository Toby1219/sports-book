from playwright.async_api import async_playwright
import asyncio
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from dataclasses import dataclass, asdict, field
import pandas as pd
import sqlite3
import os

path = 'result'
if not os.path.exists(path):
    os.mkdir(path)
else:
    pass


@dataclass
class SportBooks:
    Table_Name: str | None
    Date: float | None
    Rot: float | None
    VH: str | None
    Team: str | None
    First_1st: float | None
    Second_2nd: float | None
    Third_3rd: float | None
    Fourth_4th: float | None
    Final: float | None
    Open: float | None
    Close: float | None
    ML: float | None
    Two_H: float | None


class SportsScraper:
    def __init__(self) -> None:
        pass

    async def lunch_browser(self):
        agent = UserAgent().random
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                viewport={'width': 960, 'height': 600},
                user_agent=agent
            )
            page = await context.new_page()
            await page.goto('https://www.sportsbookreviewsonline.com/scoresoddsarchives/nba-odds-2009-10/',
                            timeout=600000)
            await page.wait_for_load_state('load', timeout=600000)

            await page.wait_for_selector('div.body-container', timeout=60000)
            html = await page.content()
            self.scraper(html)

    def scraper(self, response):
        soup = BeautifulSoup(response, 'html5lib')

        tr = soup.select('div.body-container>table>tbody>tr')
        name = soup.find('div', class_='body-container').h1.text
        data_list = SportBooksList()
        for trs in tr[1:]:
            date = trs.select_one('td:nth-child(1)').text
            rot = trs.select_one('td:nth-child(2)').text
            vh = trs.select_one('td:nth-child(3)').text
            team = trs.select_one('td:nth-child(4)').text
            first = trs.select_one('td:nth-child(5)').text
            second = trs.select_one('td:nth-child(6)').text
            third = trs.select_one('td:nth-child(7)').text
            fourth = trs.select_one('td:nth-child(8)').text
            final = trs.select_one('td:nth-child(9)').text
            open_ = trs.select_one('td:nth-child(10)').text
            close = trs.select_one('td:nth-child(11)').text
            ml = trs.select_one('td:nth-child(12)').text
            two_h = trs.select_one('td:nth-child(13)').text

            data = SportBooks(
                Table_Name=name,
                Date=date,
                Rot=rot,
                VH=vh,
                Team=team,
                First_1st=first,
                Second_2nd=second,
                Third_3rd=third,
                Fourth_4th=fourth,
                Final=final,
                Open=open_,
                Close=close,
                ML=ml,
                Two_H=two_h,
            )
            print(asdict(data))
            data_list.sb_list.append(asdict(data))
        return self.writer_(data_list)

    def writer_(self, data_list):
        data_list.save_to_csv('result/sport_books')
        data_list.save_to_excel('result/sport_books')
        data_list.save_to_json('result/sport_books')
        data_list.save_to_sqlite3('result/sport_books')


@dataclass
class SportBooksList:
    sb_list: list[SportsScraper] = field(default_factory=list)

    def dataframe(self):
        return pd.DataFrame((sbs for sbs in self.sb_list))

    def save_to_csv(self, filename):
        self.dataframe().to_csv(f'{filename}.csv', index=False)

    def save_to_excel(self, filename):
        self.dataframe().to_excel(f'{filename}.xlsx', index=False)

    def save_to_sqlite3(self, filename):
        conn = sqlite3.connect(f'{filename}.db')
        self.dataframe().to_sql(name='scraped_data', con=conn, index=False, if_exists='replace')
        conn.close()

    def save_to_json(self, filename):
        self.dataframe().to_json(f'{filename}.json', orient='records', indent=4, )


if __name__ == '__main__':
    web = SportsScraper()
    asyncio.run(web.lunch_browser())

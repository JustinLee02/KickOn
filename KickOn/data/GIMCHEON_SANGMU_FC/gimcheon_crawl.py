import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def crawl_transfermarkt_gimcheon():

    options = webdriver.ChromeOptions()

    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)

    data = []

    try:
        url = "https://www.transfermarkt.com/gimcheon-sangmu/startseite/verein/6505"
        driver.get(url)
        time.sleep(10)

        table = driver.find_element(By.CSS_SELECTOR, "table.items")

        rows = table.find_elements(By.CSS_SELECTOR, "tr.odd, tr.even")

        for row in rows:
            player_name = "N/A"
            player_birth = "N/A"
            market_value = "N/A"

            # (A) 선수 이름
            try:
                player_td = row.find_element(By.CSS_SELECTOR, "td.hauptlink a")
                player_name = player_td.text.strip()
            except:
                pass

            # (B) 생년월일
            try:
                zentriert_tds = row.find_elements(By.CSS_SELECTOR, "td.zentriert:nth-child(3)")
                player_birth = zentriert_tds[0].text.strip()
            except:
                pass

            # (C) 마켓 밸류
            try:
                mv_td = row.find_element(By.CSS_SELECTOR, "td.rechts.hauptlink a")
                market_value = mv_td.text.strip()
            except:
                pass

            print(f"Player: {player_name}, Birth: {player_birth}, Market Value: {market_value}")
            data.append([player_name, player_birth, market_value])

    finally:
        driver.quit()

    output_file = "transfermarkt_gimcheon_output.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # 헤더 작성
        writer.writerow(["Player", "Birth", "Market Value"])
        # 데이터 행 작성
        writer.writerows(data)

    print(f"CSV file saved as {output_file}")

if __name__ == "__main__":
    crawl_transfermarkt_gimcheon()
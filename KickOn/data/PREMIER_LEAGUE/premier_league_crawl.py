import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def premier_crawl_transfermarkt(url, team_name):

    options = webdriver.ChromeOptions()

    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)

    data = []

    try:
        url = url
        driver.get(url)
        time.sleep(10)

        table = driver.find_element(By.CSS_SELECTOR, "table.items")

        rows = table.find_elements(By.CSS_SELECTOR, "tr.odd, tr.even")

        for row in rows:
            season = "2020"
            player_name = "N/A"
            player_birth = "N/A"
            position = "N/A"
            # team_name = "N/A"
            appearance = "N/A"
            goals = "N/A"
            assists = "N/A"
            minutes_played = "N/A"
            market_value = "N/A"
            joined_date = "N/A"
            contract_expires = "N/A"
            team_rank = "N/A"

            profile_url = None
            # (A) 선수 이름
            try:
                player_td = row.find_element(By.CSS_SELECTOR, "td.hauptlink a")
                player_name = player_td.text.strip()
                profile_url = player_td.get_attribute("href")

            except Exception as e:
                print("Error extracting player name/profile URL:", e)

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

            if profile_url:
                try:
                    # 현재 창(스쿼드 페이지) 식별
                    original_window = driver.current_window_handle
                    # 새 탭 열기
                    driver.execute_script("window.open(arguments[0]);", profile_url)
                    time.sleep(3)
                    # 새 탭으로 스위치
                    driver.switch_to.window(driver.window_handles[-1])
                    time.sleep(3)

                    # Position
                    try:
                        label_position = driver.find_element(
                            By.XPATH,
                            "//span[@class='info-table__content info-table__content--regular' and contains(text(), 'Position:')]",
                        )
                        position_span = label_position.find_element(
                            By.XPATH,
                            "./following-sibling::span[@class='info-table__content info-table__content--bold']"
                        )
                        position = position_span.text.strip()
                        # print("Position:", position)
                    except Exception as e:
                        print("Position not find Error:", e)

                    # Joined date
                    # try:
                    #     label_joined = driver.find_element(
                    #         By.XPATH,
                    #         "//span[@class='info-table__content info-table__content--regular' and contains(text(),'Joined:')]"
                    #     )
                    #     joined_span = label_joined.find_element(
                    #         By.XPATH,
                    #         "./following-sibling::span[@class='info-table__content info-table__content--bold']"
                    #     )
                    #     joined_date = joined_span.text.strip()
                    # except Exception as inner_e:
                    #     print(f"Joined date not found for {player_name}: {inner_e}")

                    # Contract Expires
                    # try:
                    #     label_contract_expires = driver.find_element(
                    #         By.XPATH,
                    #          "//span[@class='info-table__content info-table__content--regular' and contains(text(),'Contract expires:')]")
                    #     contract_span = label_contract_expires.find_element(
                    #         By.XPATH,
                    #         "./following-sibling::span[@class='info-table__content info-table__content--bold']")
                    #     contract_expires = contract_span.text.strip()
                    # except Exception as inner_e:
                    #     print(f"Contract expires not found for {player_name}: {inner_e}")

                    driver.close()
                    driver.switch_to.window(original_window)

                except Exception as e:
                    print("Error navigating to player profile:", e)

                # 출력 확인
            print(f"Player: {player_name}, Birth: {player_birth}, Position: {position}, Market Value: {market_value}, "
                  f"Joined: {joined_date} , Contract Expires: {contract_expires} ")

            # CSV에 기록할 배열
            data.append([
               player_name, player_birth, position, market_value,
                joined_date, contract_expires
            ])

    finally:
        driver.quit()

    output_file = f"transfermarkt_{team_name}_output.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # 헤더 작성
        writer.writerow(["Player", "Birth", "Position", "Market Value", "Contract", "Joined"])
        # 데이터 행 작성
        writer.writerows(data)

    print(f"CSV file saved as {output_file}")

if __name__ == "__main__":
    url_info = {
        "Liverpool": "https://www.transfermarkt.com/fc-liverpool/startseite/verein/31/saison_id/2020"
    }
    for team_name, url in url_info.items():
        premier_crawl_transfermarkt(url, team_name)
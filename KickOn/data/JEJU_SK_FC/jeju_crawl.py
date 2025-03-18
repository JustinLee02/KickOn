import csv
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def crawl_transfermarkt_jeju():

    options = webdriver.ChromeOptions()

    service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)

    data = []

    try:
        url = "https://www.transfermarkt.com/daejeon-hana-citizen/startseite/verein/6499"
        driver.get(url)
        time.sleep(10)

        table = driver.find_element(By.CSS_SELECTOR, "table.items")

        rows = table.find_elements(By.CSS_SELECTOR, "tr.odd, tr.even")

        for row in rows:
            player_name = "N/A"
            player_birth = "N/A"
            citizenship = "N/A"
            foot = "N/A"
            contract_expires = "N/A"
            joined_date = "N/A"
            market_value = "N/A"


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

                    # Citizenship
                    try:
                        label_citizenship = driver.find_element(By.XPATH,
                                                                "//span[contains(text(),'Citizenship:') or contains(text(),'Nationalität:')]")
                        cit_span = label_citizenship.find_element(By.XPATH, "./following-sibling::span")
                        citizenship = cit_span.text.strip()
                    except Exception as inner_e:
                        print(f"Citizenship not found for {player_name}: {inner_e}")

                    # Joined date
                    try:
                        label_joined = driver.find_element(
                            By.XPATH,
                            "//span[@class='info-table__content info-table__content--regular' and contains(text(),'Joined:')]"
                        )
                        joined_span = label_joined.find_element(
                            By.XPATH,
                            "./following-sibling::span[@class='info-table__content info-table__content--bold']"
                        )
                        joined_date = joined_span.text.strip()
                        print(joined_date)
                    except Exception as inner_e:
                        print(f"Joined date not found for {player_name}: {inner_e}")

                    # Contract Expires
                    try:
                        label_contract_expires = driver.find_element(
                            By.XPATH,
                             "//span[@class='info-table__content info-table__content--regular' and contains(text(),'Contract expires:')]")
                        contract_span = label_contract_expires.find_element(
                            By.XPATH,
                            "./following-sibling::span[@class='info-table__content info-table__content--bold']")
                        contract_expires = contract_span.text.strip()
                    except Exception as inner_e:
                        print(f"Contract expires not found for {player_name}: {inner_e}")

                    # Foot
                    try:
                        label_foot = driver.find_element(By.XPATH,
                                                         "//span[contains(text(),'Foot:') or contains(text(),'Fuß:')]")
                        foot_span = label_foot.find_element(By.XPATH, "./following-sibling::span")
                        foot = foot_span.text.strip()
                    except Exception as inner_e:
                        print(f"Foot not found for {player_name}: {inner_e}")

                    driver.close()
                    driver.switch_to.window(original_window)

                except Exception as e:
                    print("Error navigating to player profile:", e)

                # 출력 확인
            print(f"Player: {player_name}, Birth: {player_birth}, Market Value: {market_value}, "
                  f"Citizenship: {citizenship}, Foot: {foot}, Contract Expires: {contract_expires}, Joined: {joined_date}")

            # CSV에 기록할 배열
            data.append([
                player_name, player_birth, market_value,
                citizenship, foot, contract_expires, joined_date
            ])

    finally:
        driver.quit()

    output_file = "transfermarkt_jeju_output.csv"
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        # 헤더 작성
        writer.writerow(["Player", "Birth", "Market Value", "Citizenship", "Foot", "Contract", "Joined"])
        # 데이터 행 작성
        writer.writerows(data)

    print(f"CSV file saved as {output_file}")

if __name__ == "__main__":
    crawl_transfermarkt_jeju()
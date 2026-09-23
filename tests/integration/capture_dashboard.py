"""Capture the dashboard only after its asynchronous data has rendered."""

from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


def dashboard_is_ready(driver: webdriver.Chrome) -> bool:
    health = driver.find_element(By.ID, "pipelineHealth").text
    price = driver.find_element(By.ID, "statPrice").text
    watchlist = driver.find_elements(By.CSS_SELECTOR, ".watchlist-item")
    return "Loading health" not in health and price not in {"", "--"} and bool(watchlist)


def main() -> None:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1440,1000")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get("http://localhost:5000")
        WebDriverWait(driver, 30).until(dashboard_is_ready)
        output = Path("dashboard.png").resolve()
        if not driver.save_screenshot(str(output)):
            raise RuntimeError("Chrome did not create the dashboard screenshot")
        print(f"Dashboard screenshot saved to {output}")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()

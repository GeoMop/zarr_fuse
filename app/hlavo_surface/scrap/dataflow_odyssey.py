import os
from datetime import datetime, timezone
import time
import requests
import re
import csv
import traceback
from pathlib import Path
import glob
import polars as pl
import numpy as np

from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import zarr_fuse
import hlavo_surface.inputs as inputs
work_dir = inputs.work_dir


def create_driver(default_download_dir):
    """
    Creates and returns a Selenium WebDriver instance with configured download settings.
    """
    # SETUP browser
    options = Options()
    options.BinaryLocation = "/snap/bin/chromium"
    options.add_experimental_option('prefs', {
        "download.default_directory": str(work_dir / default_download_dir),  # Change to your desired download folder
        "download.prompt_for_download": False,  # Disable download prompt
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    # tips for snap and chromium: https://stackoverflow.com/questions/5731953/use-selenium-with-chromium-browser
    # open chrome driver service
    chrome_service = Service(executable_path="/snap/bin/chromium.chromedriver")
    # open browser
    return webdriver.Chrome(service=chrome_service, options=options)


def login(driver):
    """
    Logs into the website using provided credentials.
    """
    # Load credentials from .env
    load_dotenv(dotenv_path=inputs.odyssey_credentials_env)
    login_email = os.getenv("DATAFLOW_EMAIL")
    login_pass = os.getenv("DATAFLOW_PASS")
    if not login_email or not login_pass:
        raise ValueError("Missing login credentials in the .env file.")

    # Fill in the username and password
    wait = WebDriverWait(driver, timeout=10)
    username_field = wait.until(EC.presence_of_element_located((By.ID, "email")))
    # username_field = driver.find_element(By.ID, "email")
    password_field = driver.find_element(By.ID, "password")
    login_button = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")

    username_field.send_keys(login_email)
    password_field.send_keys(login_pass)
    time.sleep(0.5)
    login_button.click()
    print(f"Logged in.")


def fill_in_form(driver, date_interval, logger_group):
    """
    Fills in the date interval and selects the logger group from the dropdown.
    """
    # <select class="form-control m-b" name="report_loggers_dropdown" id="report_loggers_dropdown">
    #   <option>Select Logger</option><option value="4785">U11 - 364C - Multi Profile Soi Moisture and Temperature</option>
    #   ...
    # </select>
    # < select class ="form-control m-b group_selector" name="report_groups_dropdown" id="report_groups_dropdown" >
    #   < option > Select Group < / option >
    #   < option value="831" > Uhelná < / option >
    # ...
    #   < / select >
    wait = WebDriverWait(driver, timeout=10)
    location_dropdown = Select(wait.until(EC.presence_of_element_located((By.ID, "report_groups_dropdown"))))
    # there is a delay until all Select options are ready
    time.sleep(5)
    wait.until(EC.presence_of_element_located((By.XPATH, f"//option[text()='{logger_group}']")))
    # location_dropdown = Select(driver.find_element(By.ID, "report_groups_dropdown"))
    location_dropdown.select_by_visible_text(logger_group)
    time.sleep(0.5)

    # <input type="text" class="form-control hasDatepicker" id="report_from_date" placeholder="From" required="">
    # <input type="text" class="form-control hasDatepicker" id="report_to_date" placeholder="To" required="">
    date_from_input = driver.find_element(By.ID, "report_from_date")
    date_from_input.clear()
    dt = datetime.strptime(date_interval['start_date'], '%Y-%m-%d').date()
    dt_str = dt.strftime('%d-%m-%Y')
    date_from_input.send_keys(dt_str)

    date_to_input = driver.find_element(By.ID, "report_to_date")
    date_to_input.clear()
    dt = datetime.strptime(date_interval['end_date'], '%Y-%m-%d').date()
    dt_str = dt.strftime('%d-%m-%Y')
    date_to_input.send_keys(dt_str)


def gather_logger_info(driver, logger_group, download_dir):
    """
    Selects the logger group from the dropdown and goes through loggers.
    """
    # <select class="form-control m-b group_selector" name="groups_tab_groups_dropdown" id="groups_dropdown">
    #   <option>Select Group</option>
    #   <option selected="selected" value="601">Home</option>
    #   <option value="618">Lab</option>
    #   ...
    #   < / select >
    # <select class="form-control m-b" name="loggers_dropdown" id="loggers_dropdown">
    #   <option>Select Logger</option>
    #   <option value="3093">U01 - E9F7 - Multi Profile Soi Moisture and Temperature</option>
    #   ...
    #   </select>
    print(f"Gathering loggers info.")
    wait = WebDriverWait(driver, timeout=10)
    location_dropdown = Select(wait.until(EC.presence_of_element_located((By.ID, "groups_dropdown"))))
    # there is a delay until all Select options are ready
    time.sleep(5)
    wait.until(EC.presence_of_element_located((By.XPATH, f"//option[text()='{logger_group}']")))
    # location_dropdown = Select(driver.find_element(By.ID, "report_groups_dropdown"))
    location_dropdown.select_by_visible_text(logger_group)
    time.sleep(5)

    logger_dropdown = Select(driver.find_element(By.ID, "loggers_dropdown"))
    # time.sleep(5)
    # print(f"Waiting for logger_dropdown.")
    for i in range(60):
        if len(logger_dropdown.options)>1:
            # print(f"logger_dropdown ready.")
            break
        time.sleep(0.5)

    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M:%S")

    # Iterate over each option in the logger dropdown
    print(f"Going through loggers.")
    rows = []
    for idx, option in enumerate(logger_dropdown.options):
        # Skip the "Select Logger" placeholder
        selected_text = option.text
        if selected_text == "Select Logger":
            continue

        print(f"{idx}/{len(logger_dropdown.options)-1} {selected_text[:10]}")

        logger_dropdown.select_by_visible_text(selected_text)
        time.sleep(0.5)

        # Extract number
        def extract_num(text):
            match = re.search(r"([-+]?\d*\.\d+|\d+)", text)
            if match:
                return match.group()
            else:
                return "0"

        # wait until the logger data are loaded
        # i.e. until the loggerUid is the same in dropdown and in info div
        # print(f"Waiting for logger_name.")
        for i in range(40):
            logger_name = driver.find_element(By.ID, "logger_name_info_box")
            # print(f"logger_name_info_box.")
            logger_type = driver.find_element(By.ID, "logger_type_info_box")
            # print(f"logger_type_info_box.")
            # resolve empty info div
            if ': ' not in logger_name.text or ': ' not in logger_type.text:
                time.sleep(0.5)
                continue
            text = logger_name.text.split(": ", 1)[1] + " - " + logger_type.text.split(": ", 1)[1]
            # print(f"compare text.")
            if selected_text == text:
                # print(f"logger_name ready.")
                break
            time.sleep(0.5)
            # print(f"next.")

        logger_uid = selected_text[:10].replace(" ", "")
        latitude_div = driver.find_element(By.ID, "logger_lat_info_box")
        longitude_div = driver.find_element(By.ID, "logger_lng_info_box")
        latitude = extract_num(latitude_div.text)
        longitude = extract_num(longitude_div.text)

        rows.append([current_date, current_time, logger_uid, latitude, longitude])

    with open(download_dir / f"logger_gps_list_{logger_group}.csv", mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['date', 'time', 'loggerUid', 'latitude', 'longitude'])
        for r in rows:
            writer.writerow(r)


def merge_logger_info_groups(download_dir):
    # Pattern to match the files
    csv_files = sorted(
        download_dir.glob("logger_gps_list_*.csv"),
        key=lambda f: f.stat().st_mtime  # Sort by modification time
    )
    if len(csv_files) < 2:
        return

    # Read and concatenate all CSV files
    df_list = [pl.read_csv(file) for file in csv_files]
    merged_df = pl.concat(df_list)

    # Save to a new merged file
    merged_df.to_csv(download_dir / "logger_gps_list_all.csv", index=False)


def submit_loggers(driver):
    """
    Iterates through loggers in the dropdown, submits reports, and confirms success.
    """
    logger_dropdown = Select(driver.find_element(By.ID, "report_loggers_dropdown"))
    time.sleep(1)
    logger_count = 0

    # Iterate over each option in the dropdown
    for option in logger_dropdown.options:
        # Skip the "Select Logger" placeholder
        if option.text == "Select Logger":
            continue

        logger_dropdown.select_by_visible_text(option.text)
        time.sleep(0.5)

        # <input class="btn btn-primary" id="btn_report_submit" type="submit" value="Submit">
        submit_btn = driver.find_element(By.ID, "btn_report_submit")
        submit_btn.click()
        # time.sleep(0.5)

        # wait for OK button
        # <div class="jconfirm-box></div>
        # <div class="jconfirm-title-c"><span class="jconfirm-icon-c"></span>
        # <span class="jconfirm-title">Processing Data... Please wait.</span></div>
        # <div class="jconfirm-content-pane" ...>
        # <div class="jconfirm-content" id="jconfirm-box34841">Press the refresh button shortly.</div></div>
        # <div class="jconfirm-buttons"><button type="button" class="btn btn-default">ok</button></div><div class="jconfirm-clear"></div></div>
        # Wait for the message box to appear
        wait = WebDriverWait(driver, timeout=10)
        message_box = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "jconfirm-box")))
        # Wait for the "OK" button inside the message box
        ok_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='ok']")))
        ok_button.click()

        logger_count += 1
        # for testing:
        # if logger_count == 2:
        #     break

    return logger_count


def download_file(file_url, file_name, download_dir):
    """
    Handles downloading a file from a given URL and saves it to the specified directory.
    """
    try:
        print(f"Downloading {file_name} from {file_url}...")
        response = requests.get(file_url, timeout=30)
        if response.status_code == 200:
            file_path = download_dir / file_name
            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"Downloaded: {file_name}")
            return True
        else:
            print(f"Failed to download {file_name} (Status: {response.status_code})")
            return False
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {file_name}: {e}")
        return False


def download_reports(driver, download_dir, logger_count, current_dt):
    """
    Iterates through table rows, retrieves file links, and downloads them.
    """
    n_found_links = 0
    for i in range(10): # n tryouts
        # <table id="reports_table">
        #     <thead><tr role="row"><th class="sorting"...</th></tr></thead>
        #     <tbody><tr role="row" class="odd">
        #       <td><a href="https://clientreportsprocessed.s3.ap-southeast-2.amazonaws.com/jan.brezina@tul.cz:U11__364C:1738068349011.csv">
        #          jan.brezina@tul.cz:U11__364C:1738068349011.csv</a></td>
        #       <td class="sorting_1">2025-01-28 12:45:52 UTC</td><td>51824</td></tr><tr role="row" class="even">
        #     </tr></tbody></table>
        wait = WebDriverWait(driver, timeout=10)
        table = wait.until(EC.presence_of_element_located((By.ID, 'reports_table')))
        # table = driver.find_element(By.ID, "reports_table")

        # display 50 rows
        # <select name="reports_table_length" aria-controls="reports_table" class="">
        # <option value="10">10</option><option value="25">25</option><option value="50">50</option><option value="100">100</option>
        # </select>

        # Set display rows to 50
        num_entries_dropdown = Select(wait.until(EC.presence_of_element_located((By.NAME, 'reports_table_length'))))
        # num_entries_dropdown = Select(driver.find_element(By.NAME, "reports_table_length"))
        num_entries_dropdown.select_by_value("50")

        # Locate all rows inside the table's tbody
        rows = table.find_elements(By.XPATH, ".//tbody/tr")

        # count refreshed table rows of submitted loggers
        n_found_links = 0
        for row in rows:
            dt_element = row.find_element(By.XPATH, ".//td[2]")
            dt = datetime.strptime(dt_element.text.strip(), "%Y-%m-%d %H:%M:%S %Z")
            # dt.replace(tzinfo=timezone.utc)
            # current_dt = current_dt.replace(tzinfo=None)
            dt = dt.replace(tzinfo=timezone.utc)
            time_diff = (current_dt - dt).total_seconds()/60
            if time_diff < 2:
                n_found_links += 1

        if n_found_links < logger_count:
            refresh_btn = driver.find_element(By.ID, "btn_report_refresh")
            print(f"refresh link table {i}")
            refresh_btn.click()
            time.sleep(2)
            continue

        counter = 0
        for row in rows:
            link_element = row.find_element(By.XPATH, ".//td[1]/a")
            file_url = link_element.get_attribute("href")
            logger_name = link_element.text.strip().split(':')[1]
            logger_name = logger_name.replace('__', '-')

            res = download_file(file_url, f"{logger_name}.csv", download_dir)
            counter += 1

            if not res or counter == logger_count:
                break
        break

    if n_found_links != logger_count:
        raise Exception("Not all submitted loggers found in the data reports link table.")


def run_dataflow_extraction(download_dir, date_interval, logger_groups, flags):
    """
    Runs the full extraction process from login to downloading reports.
    """
    try:
        driver = create_driver(default_download_dir=download_dir)
        wait = WebDriverWait(driver, timeout=10)
        time.sleep(1)

        # Navigate to the website, returns once loaded
        driver.get('https://www.xpert.nz/home')

        login(driver)

        for logger_group in logger_groups:
            print(f"DataFlow extraction for group '{logger_group}'")

            if flags['location']:
                # wait for "Map" tab
                tab = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href="#tab-1"]')))
                tab.click()
                print(f"Tab '{tab.text}' Click")
                gather_logger_info(driver, logger_group, download_dir)
                # continue

            if flags['data_reports']:
                # wait for "Data reports" tab
                tab = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href="#reports"]')))
                # tab = driver.find_element(By.CSS_SELECTOR, 'a[href="#reports"]')
                print(f"Tab '{tab.text}' Click")
                tab.click()

                fill_in_form(driver, date_interval, logger_group)
                logger_count = submit_loggers(driver)
                # logger_count = 4  # testing without submitting
                # wait before refreshing the report links table
                time.sleep(2)

                # <input class="btn btn-success" id="btn_report_refresh" type="submit" value="Refresh">
                refresh_btn = driver.find_element(By.ID, "btn_report_refresh")
                print("refresh link table")
                refresh_btn.click()

                current_dt = datetime.now(tz=timezone.utc)
                download_reports(driver, download_dir, logger_count, current_dt)

        merge_logger_info_groups(download_dir)

    except Exception: traceback.print_exc()
    finally:
        print("DataFlow webpage extraction FINISHED")
        driver.quit()  # Ensure the browser is closed after the script runs


def parse_datetime_column(column: pl.Series) -> pl.Series:
    """
    Parses a datetime column with varying formats into a standardized datetime object.

    Args:
        column (pd.Series): A Pandas Series containing datetime strings or timestamps.

    Returns:
        pd.Series: A Series with standardized datetime objects.
    """
    def detect_and_parse(value):
        try:
            # Format 1: "25-09-2024 08:20:00"
            return datetime.strptime(value, '%d-%m-%Y %H:%M:%S')
        except ValueError:
            pass

        try:
            # Format 2: "2024-09-25T16:50:24" (ISO format)
            return datetime.fromisoformat(value)
        except ValueError:
            pass

        try:
            # Format 3: Unix timestamp in milliseconds
            timestamp = int(value)
            # return datetime.fromtimestamp(timestamp / 1000)
            return pl.Datetime("ms")
        except (ValueError, OverflowError):
            pass

        # If all parsing attempts fail, return NaT
        return None

    # Apply the function and return as Polars Series
    parsed = [detect_and_parse(v) for v in column]
    return pl.Series(name=column.name, values=parsed).cast(pl.Datetime("ms"))


def read_data(file, dt_column='DateTime', sep=';'):
    """
    Reads data from CSV file with file structure given by :param:`file_pattern`
    """
    # Read each CSV file and append to the list
    df = pl.read_csv(file, separator=sep)

    # Parse the datetime column
    parsed_series = parse_datetime_column(df[dt_column])
    df = df.with_columns(parsed_series)
    df = df.filter(df[dt_column].is_not_null())

    # Drop unused columns
    df = df.drop(col for col in df.columns if col in ["ttlDate", "logDateTime"])

    # Replace invalid strings with nulls and cast to Float64
    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False) for col in df.columns if col not in [dt_column, "loggerUid"]
    ])

    # Sort the DataFrame by DateTime
    df = df.sort(dt_column)
    return df


def read_odyssey_data(download_dir, df_locs: pl.DataFrame):
    ods_data = []
    file_pattern = str(download_dir / 'U*-*.csv')
    csv_files = glob.glob(file_pattern, recursive=False)

    # for each Odyssey logger
    for file in csv_files:
        logger_uid = Path(file).stem
        # column dateTtime is UTC time
        # it corresponds to column M2(logDateTime) =((M2/1000)/(24*60*60))+DATE(1970;1;1)
        df = read_data(file, dt_column='dateTime', sep=',')
        # save the full logger Uid
        df = df.rename(mapping={"loggerUid": f"fullLoggerUid"})
        # Add a new column with constant string
        df = df.with_columns([
            pl.lit(logger_uid).alias("loggerUid")
        ])
        for i in range(5):
            # moisture - rescale to [0,1]
            df = df.with_columns([
                (pl.col(f"s{i+1}") / 100)
            ])
            # temperature
            df = df.rename(mapping={f"s{i+1}t": f"t{i+1}"})
        # ambient temperature
        df = df.rename(mapping={f"s11t": f"t0"})

        # Add location columns
        # print(df_locs.filter(pl.col("logger_id") == logger_uid))
        lat = df_locs.filter(pl.col("logger_id") == logger_uid).select("latitude").item()
        lon = df_locs.filter(pl.col("logger_id") == logger_uid).select("longitude").item()
        df = df.with_columns([
            pl.lit(lat).alias("latitude"),
            pl.lit(lon).alias("longitude")
        ])
        ods_data.append(df)

    final_df = pl.concat(ods_data)
    return final_df


def location_df(attrs_dict):

    raw_df = pl.read_csv(inputs.input_dir / attrs_dict['location_file'], has_header=True, skip_rows=1)
    # print(raw_df.select(["logger_id", "latitude", "longitude"]))

    col_logger_id = "logger_id"
    df = raw_df.filter(
        (pl.col(col_logger_id).is_not_null()) & (pl.col(col_logger_id) != "")
    )

    df = df.drop(col for col in df.columns if col not in [col_logger_id, "latitude", "longitude"])

    # Replace invalid strings with nulls and cast to Float64
    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False) for col in df.columns if col != col_logger_id
    ])

    # print(df)
    return df



# Main script only defines parameters
if __name__ == '__main__':
    schema = zarr_fuse.schema.deserialize(inputs.surface_schema_yaml)
    df_locs = location_df(schema['ATTRS'])

    download_dir = work_dir / \
                   (datetime.now().strftime("%Y%m%dT%H%M%S") + "_dataflow_scrape")
    # download_dir.mkdir()
    download_dir = work_dir / "20250506T211454_dataflow_scrape"

    # logger_groups = ["Uhelná lesík", "Lab", "Uhelná"]
    logger_groups = ["Uhelná lesík"]
    date_interval = {'start_date': '2025-01-01', 'end_date': '2025-04-22'}

    flags = {'location': False, 'data_reports': True}
    # Run the extraction
    # run_dataflow_extraction(download_dir, date_interval, logger_groups, flags)

    # read and transform CSV
    ods_df = read_odyssey_data(download_dir, df_locs)
    print(ods_df)

    root_node = zarr_fuse.open_storage(schema, workdir=work_dir)
    odyssey_node = root_node['odyssey']
    odyssey_node.update(ods_df)

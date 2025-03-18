import os
import sqlite3
import chromadb
import openai
import asyncio
import nest_asyncio
import random
import requests
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# Apply nest_asyncio to allow async functions in synchronous contexts
nest_asyncio.apply()

# Set database paths
SQLITE_DB_PATH = "" #add path
CHROMA_DB_PATH = "" #add path

# Load .env file
load_dotenv()

# Retrieve API keys
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
BROWSERBASE_API_KEY = os.getenv("BROWSERBASE_API_KEY", "KEY") #paste key
BROWSERBASE_PROJECT_ID = os.getenv("BROWSERBASE_PROJECT_ID", "ID") #paste id

if not OPENROUTER_API_KEY:
    raise ValueError("ERROR: OpenRouter API key is missing. Set OPENROUTER_API_KEY properly.")

# Connect to ChromaDB (update to supabase integration)
chroma_client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
collection = chroma_client.get_collection("documents")

# FastAPI setup
app = FastAPI()

class APNRequest(BaseModel):
    apn: str

# Database Search Functions

def parcel_local_search(apn):
    """Search SQLite database for property details."""
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    #specify column order
    cursor.execute("""
        SELECT AIN, SitusFullA, TaxRateAre, SQFTmain1, LegalDescr, FLD_ZONE, 
               ZONE_SUBTY, NAME, PLNG_AREA, TITLE_22, Zone_Type_1, Zone_Type_2, Zone_Type_3, Zone_Type_4, Zone_Type_5, Seismic_Quadrangle 
        FROM final_combined WHERE AIN=?
    """, (apn,))
    
    result = cursor.fetchone()
    conn.close()

    #col list
    columns = ["AIN", "SitusFullA", "TaxRateAre", "SQFTmain1", "LegalDescr", "FLD_ZONE",
               "ZONE_SUBTY", "NAME", "PLNG_AREA", "TITLE_22", 
               "Zone_Type_1", "Zone_Type_2", "Zone_Type_3", "Zone_Type_4", "Zone_Type_5", "Seismic_Quadrangle"]

    #ensure result not None before zip
    return dict(zip(columns, result)) if result else None

def retrieve_context(query):
    """Retrieve top 3 most relevant text chunks from ChromaDB.""" #update chunking
    try:
        results = collection.query(query_texts=[query], n_results=3)
        if results and "documents" in results and results["documents"]:
            return "\n".join(results["documents"][0])
        else:
            return "No relevant documents found."
    except Exception as e:
        print(f"Error retrieving context from ChromaDB: {e}")
        return "Error retrieving context."

def call_gemini_flash(parcel_data, context):
    
    system_prompt = """
    I want a breakdown of the zoning for any given property API. The summary should highlight the current zoning code the property falls under and break down every possible use of the property from the current zoning regulations. The breakdown should also include earthquake and flood zones.

For the zoning uses of the API, return them in a list format, not in paragraph format. It should site the zoning code that it falls under, then break down the regulations that fall under that code. The same should be done for the earthquake and flood zones. If there are any historic preservation over zones, include a link to the corresponding page to that zone. If there is a hillside ordinance, include the link to the corresponding ordinance.

Be careful not to include codes that do not exist. Only include codes that do exist.

For context, I want a zoning summary that can be used to assist real estate appraisers so that they do not have to search through the zoning code themselves. This report should be in-depth enough so that a licensed appraiser can use this summary to complete their reports and save them time. """

    user_prompt = f"""
    Property Details:
    {parcel_data}

    Relevant Documents:
    {context}

    Explain the zoning details, flood risk, tax rate area, and any relevant regulations based on the retrieved information.
    """

    client = openai.OpenAI(
        api_key=OPENROUTER_API_KEY,
        base_url="https://openrouter.ai/api/v1"  
    )

    response = client.chat.completions.create(
        model="google/gemini-pro",  # Updated model name
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    )

    return response.choices[0].message.content

#Web Scraper Functions

def get_random_user_agent():
    """Returns a random User-Agent string."""
    user_agents = [ #insert user agents 
        
    ]
    return random.choice(user_agents)

async def random_delay(min_time=3, max_time=6):
    """Adds a random delay between actions."""
    delay = random.uniform(min_time, max_time)
    print(f"Sleeping for {delay:.2f} seconds...")
    await asyncio.sleep(delay)

async def random_scroll(page):
    """Scrolls randomly on the page."""
    scroll_height = await page.evaluate("document.body.scrollHeight")
    scroll_position = random.randint(0, scroll_height)
    await page.evaluate(f"window.scrollTo(0, {scroll_position});")
    print(f"Scrolled to position: {scroll_position}")
    await random_delay(1, 3)

def create_browserbase_session(api_key, project_id):
    """Creates new Browserbase session."""
    url = "https://api.browserbase.com/v1/sessions"
    headers = {"Content-Type": "application/json", "x-bb-api-key": api_key}
    payload = {"projectId": project_id, "proxies": True}

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in [200, 201]:
        print("Session created successfully.")
        return response.json()
    else:
        print(f"Failed to create session: {response.status_code} - {response.text}")
        return None

async def scrape_and_extract_zones(apn, connect_url):
    """Scrapes zone information and extracts zones and flood hazard using Playwright with Browserbase."""
    async with async_playwright() as p:
        # connect to Browserbase
        browser = await p.chromium.connect_over_cdp(connect_url)
        context = await browser.new_context(
            user_agent=get_random_user_agent(),
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        try:
            # step 1: scrape the parcel profile report link
            print("Navigating to LADBS Atlas...")
            await page.goto("https://ladbs.org/atlas/", timeout=60000)
            print("Page title:", await page.title())

            if "Service unavailable" in await page.title():
                print("Page returned 'Service unavailable'.")
                return {"error": "Service unavailable"}

            # locate the iframe
            print("Waiting for iframe...")
            iframe = page.frame(url="https://experience.arcgis.com/experience/31c73214b7034356a3cd4903bab233f7/page/Page")
            if iframe is None:
                raise Exception("Iframe not found.")
            print("Iframe found!")

            # handle acknowledgment box
            try:
                acknowledgment = iframe.locator("#jimu-link-app-2 > span.touch-ripple-root")
                await acknowledgment.wait_for(state="visible", timeout=60000)
                await acknowledgment.click()
                print("Acknowledgment clicked.")
            except Exception as e:
                print("Acknowledgment box not found or already dismissed:", e)

            # interact with the search box
            search_box = iframe.locator("input[placeholder*='Search by Address']")
            await search_box.fill(apn)
            print(f"APN {apn} entered.")
            await search_box.press("Enter")
            print("Enter key pressed.")
            await random_delay()

            # wait for the calcite-flow-item to appear
            print("Waiting for calcite-flow-item...")
            await iframe.locator("calcite-flow-item").wait_for(state="attached", timeout=60000)
            await iframe.locator("calcite-flow-item").scroll_into_view_if_needed()
            await iframe.locator("calcite-flow-item").wait_for(state="visible", timeout=60000)
            print("calcite-flow-item located.")

            # wait for the "Complete Parcel Profile Report" link to be visible
            print("Waiting for the parcel report link...")
            parcel_report_link = iframe.locator("calcite-flow-item a[href*='ParcelProfileDetail2']")
            await parcel_report_link.wait_for(state="visible", timeout=60000)
            print("Parcel report link found.")

            # get and clean the href attribute
            href = await parcel_report_link.get_attribute("href")
            href_cleaned = href.replace(" ", "")  # Remove all spaces
            print(f"Cleaned Parcel report link: {href_cleaned}")

            # step 2: extract zones and flood hazard
            print(f"Navigating to {href_cleaned}...")
            await page.goto(href_cleaned, timeout=90000)
            print("Page title:", await page.title())

            # ensure the page is fully loaded
            await page.wait_for_load_state("networkidle")
            print("Page fully loaded.")

            # extract page source and parse with BeautifulSoup
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')

            # find the table with id 'basic'
            table = soup.find("table", id="basic")
            if not table:
                raise Exception("Table with id 'basic' not found")

            # extract zones dynamically
            zones = []
            zone_cell = table.find(lambda tag: tag.name == "td" and "Zone(s):" in tag.get_text())
            if zone_cell:
                # get the rowspan value (if it exists) or default to 1
                rowspan = int(zone_cell.get('rowspan', 1))

                # get the first zone from the current row
                current_row = zone_cell.find_parent("tr")
                zone_value_cell = current_row.find_all("td")[-1]
                if zone_value_cell:
                    zones.append(zone_value_cell.get_text(strip=True))

                # get other zones from following rows
                for _ in range(rowspan - 1):  # -1 because we already got the first zone
                    current_row = current_row.find_next_sibling("tr")
                    if current_row:
                        zone_value_cell = current_row.find("td")
                        if zone_value_cell:
                            zones.append(zone_value_cell.get_text(strip=True))

            print("All zones extracted:", zones)

            # extract flood hazard zone
            flood_hazard_cell = None
            for td in table.find_all("td"):
                if "Flood Hazard Zone:" in td.get_text(strip=True):
                    flood_hazard_cell = td
                    break

            if flood_hazard_cell:
                flood_hazard_zone = flood_hazard_cell.find_next_sibling("td").get_text(strip=True)
            else:
                flood_hazard_zone = "Not Found"

            print("Flood hazard zone extracted:", flood_hazard_zone)

            result = {
                "apn": apn,
                "link": href_cleaned,
                "zones": zones,
                "flood_hazard_zone": flood_hazard_zone,
            }
            return result

        except Exception as e:
            print(f"Error: {str(e)}")
            # debug: save a screenshot of the current page
            await page.screenshot(path="error_debug_combined.png")
            return {"error": str(e)}

        finally:
            await browser.close()

async def get_web_scraped_data(apn):
    """Main function to handle web scraping for an APN."""
    # create a Browserbase session
    session = create_browserbase_session(BROWSERBASE_API_KEY, BROWSERBASE_PROJECT_ID)
    
    if not session:
        return {"error": "Failed to create Browserbase session"}
    
    connect_url = session["connectUrl"]
    result = await scrape_and_extract_zones(apn, connect_url)
    return result

# api endpoints

@app.post("/")
async def get_parcel_details_and_explanation(request: APNRequest):
    """Retrieve property details, fetch related documents, and generate an explanation.
    Falls back to web scraping if the APN is not found in the database."""
    
    # first try to get data from the database
    parcel_data = parcel_local_search(request.apn)

    if parcel_data:
        # database search successful, proceed with the original flow
        query_text = f"Zoning and regulations for {parcel_data.get('SitusFullA', 'this area')}"
        context = retrieve_context(query_text)
        explanation = call_gemini_flash(parcel_data, context)

        return {
            "parcel_data": parcel_data,
            "context": context,
            "explanation": explanation,
            "source": "database"
        }
    else:
        # APN not found in database, fall back to web scraping
        print(f"APN {request.apn} not found in database. Falling back to web scraping...")
        web_data = await get_web_scraped_data(request.apn)
        
        # return the web scraping result in info availability format
        web_data["source"] = "webscraper"
        return web_data

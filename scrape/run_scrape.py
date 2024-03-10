# Autorship information
__author__ = "Hüsamettin Deniz Özeren"
__copyright__ = "Copyright 2024"
__maintainer__ = "Hüsamettin Deniz Özeren"

import asyncio
import json
from typing import List
from httpx import AsyncClient, Response
from parsel import Selector
from typing import TypedDict
import jmespath
from pathlib import Path
from urllib.parse import urlencode
import pandas as pd
import time
from tqdm import tqdm
import os
import sys

# 1. establish HTTP client with browser-like headers to avoid being blocked
client = AsyncClient(
    headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9,lt;q=0.8,et;q=0.7,de;q=0.6",
    },
    follow_redirects=True,
    http2=True,  # enable http2 to reduce block chance
    timeout=30,
)

BASE_CONFIG = {
    "asp": True,
    "country": "GB",
    "cache": True
}

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)


class PropertyResult(TypedDict):
    """this is what our result dataset will look like"""
    id: str
    available: bool
    archived: bool
    phone: str
    bedrooms: int
    bathrooms: int
    type: str
    property_type: str
    tags: list
    description: str
    title: str
    subtitle: str
    price: str
    price_sqft: str
    address: dict
    latitude: float
    longitude: float
    features: list
    history: dict
    photos: list
    floorplans: list
    agency: dict
    industryAffiliations: list
    nearest_airports: list
    nearest_stations: list
    sizings: list
    brochures: list


def parse_property(data) -> PropertyResult:
    """parse rightmove cache data for proprety information"""
    # here we define field name to JMESPath mapping
    parse_map = {
        "id": "id",
        "available": "status.published",
        "archived": "status.archived",
        "phone": "contactInfo.telephoneNumbers.localNumber",
        "bedrooms": "bedrooms",
        "bathrooms": "bathrooms",
        "type": "transactionType",
        "property_type": "propertySubType",
        "tags": "tags",
        "description": "text.description",
        "title": "text.pageTitle",
        "subtitle": "text.propertyPhrase",
        "price": "prices.primaryPrice",
        "price_sqft": "prices.pricePerSqFt",
        "address": "address",
        "latitude": "location.latitude",
        "longitude": "location.longitude",
        "features": "keyFeatures",
        "history": "listingHistory",
        "photos": "images[*].{url: url, caption: caption}",
        "floorplans": "floorplans[*].{url: url, caption: caption}",
        "agency": """customer.{
            id: branchId, 
            branch: branchName, 
            company: companyName, 
            address: displayAddress, 
            commercial: commercial, 
            buildToRent: buildToRent,
            isNew: isNewHomeDeveloper
        }""",
        "industryAffiliations": "industryAffiliations[*].name",
        "nearest_airports": "nearestAirports[*].{name: name, distance: distance}",
        "nearest_stations": "nearestStations[*].{name: name, distance: distance}",
        "sizings": "sizings[*].{unit: unit, min: minimumSize, max: maximumSize}",
        "brochures": "brochures",
    }
    results = {}
    for key, path in parse_map.items():
        value = jmespath.search(path, data)
        results[key] = value
    return results


# This function will find the PAGE_MODEL javascript variable and extract it 
def extract_property(response: Response) -> dict:
    """extract property data from rightmove PAGE_MODEL javascript variable"""
    selector = Selector(response.text)
    data = selector.xpath("//script[contains(.,'PAGE_MODEL = ')]/text()").get()
    if not data:
        print(f"page {response.url} is not a property listing page")
        return
    data = data.split("PAGE_MODEL = ", 1)[1].strip()
    data = json.loads(data)
    return data["propertyData"]


# this is our main scraping function that takes urls and returns the data
async def scrape_properties(urls: List[str]) -> List[dict]:
    """Scrape Rightmove property listings for property data"""
    to_scrape = [client.get(url) for url in urls]
    properties = []
    for response in asyncio.as_completed(to_scrape):
        response = await response
        properties.append(parse_property(extract_property(response)))
    return properties


async def find_locations(query: str) -> List[str]:
    """use rightmove's typeahead api to find location IDs. Returns list of location IDs in most likely order"""
    # rightmove uses two character long tokens so "cornwall" becomes "CO/RN/WA/LL"
    tokenize_query = "".join(c + ("/" if i % 2 == 0 else "") for i, c in enumerate(query.upper(), start=1))
    url = f"https://www.rightmove.co.uk/typeAhead/uknostreet/{tokenize_query.strip('/')}/"
    response = await client.get(url)
    data = json.loads(response.text)
    return [prediction["locationIdentifier"] for prediction in data["typeAheadLocations"]]


async def scrape_search(location_id: str) -> dict:
    RESULTS_PER_PAGE = 24

    def make_url(offset: int) -> str:
        url = "https://www.rightmove.co.uk/api/_search?"
        params = {
            "areaSizeUnit": "sqft",
            "channel": "BUY",  # BUY or RENT
            "currencyCode": "GBP",
            "includeSSTC": "false",
            "index": offset,  # page offset
            "isFetching": "false",
            "locationIdentifier": location_id, # e.g.: "REGION^61294", 
            "numberOfPropertiesPerPage": RESULTS_PER_PAGE,
            "radius": "0.0",
            "sortType": "6",
            "viewType": "LIST",
        }
        return url + urlencode(params)
    first_page = await client.get(make_url(0))
    first_page_data = json.loads(first_page.content)
    total_results = int(first_page_data['resultCount'].replace(',', ''))
    results = first_page_data['properties']
    
    other_pages = []
    # rightmove sets the API limit to 1000 properties - Husam: This is open for testing
    max_api_results = 1000    
    for offset in range(RESULTS_PER_PAGE, total_results, RESULTS_PER_PAGE):
        # stop scraping more pages when the scraper reach the API limit
        if offset >= max_api_results:
            break
        other_pages.append(client.get(make_url(offset)))
    for response in asyncio.as_completed(other_pages):
        response = await response
        data = json.loads(response.text)
        results.extend(data['properties'])
    return results


# Eexample run:
async def run():
    data = await scrape_properties(["https://www.rightmove.co.uk/properties/143820089#/"])
    print(json.dumps(data, indent=2))


# Example run:
async def run2():
    location_id = (await find_locations("edinburgh"))[0]
    print(location_id)
    location_results = await scrape_search(location_id)
    print(json.dumps(location_results, indent=2))


async def run3():

    print("running rightmove scrape and saving results to ./results directory")

    properties_data = await scrape_properties(
        urls=["https://www.rightmove.co.uk/properties/129828533#/"]
    )
    with open(output.joinpath("properties.json"), "w", encoding="utf-8") as file:
        json.dump(properties_data, file, indent=2, ensure_ascii=False)

    cornwall_id = (await find_locations("cornwall"))[0]
    cornwall_results = await scrape_search(
        cornwall_id, max_properties=50, scrape_all_properties=False
    )
    with open(output.joinpath("search.json"), "w", encoding="utf-8") as file:
        json.dump(cornwall_results, file, indent=2, ensure_ascii=False)


async def run_save():

    print("running rightmove scrape and saving results to " + os.path.join('results', sys.argv[1] + '.csv') + " directory")

    location_id = (await find_locations(sys.argv[1]))[0]
    location_results = await scrape_search(location_id)
    # print(json.dumps(location_results, indent=2))

    feature_list = []
    for ad in tqdm(location_results,
                   desc="Scraping...",
                   ascii=False):
        # print("Scraping location id {} ...".format(ad['id']))
        property1 = await scrape_properties(["https://www.rightmove.co.uk/properties/{}#/".format(ad['id'])])
        feature_list.append(property1[0])

    #print(feature_list)
    df = pd.DataFrame(feature_list)
    df.to_csv(os.path.join('results', sys.argv[1] + '.csv'), index=False)

if __name__ == "__main__":
    asyncio.run(run_save())
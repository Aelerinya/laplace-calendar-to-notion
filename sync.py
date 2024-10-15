import json
import os
from datetime import date, datetime, timedelta
import re
from typing import List, Dict, Any, Optional, TypedDict, cast
from notion_client import Client
from dotenv import load_dotenv
from icalendar import Calendar

from dateutil import tz
import urllib.request  # Import the built-in http request module
from dateutil import parser
import logging  # Add this import at the top of the file

# Set up logging configuration
logging.basicConfig(level=logging.INFO)  # You can adjust the level as needed

# Load environment variables from .env file
load_dotenv()


# Define a TypedDict for the event structure
class GCalEvent(TypedDict):
    summary: str
    start: date
    end: date
    description: str
    id: str


# Event which is specifically a guest stay
class GCalStay(TypedDict):
    summary: str
    start: date
    end: date
    description: str
    guest: str
    id: str


# Define a TypedDict for the structure of the typed_object
class NotionStay(TypedDict):
    id: str
    Paid: bool
    Start: date
    End: date
    Guest: str
    Name: Optional[str]
    GCalID: str


class NotionGuest(TypedDict):
    id: str
    name: str


def get_calendar_events(days_ago: int | None = None) -> List[GCalEvent]:
    # Get the iCal URL from environment variables
    ical_url = os.environ.get("GOOGLE_CALENDAR_ICAL_URL")

    if not ical_url:
        raise ValueError(
            "GOOGLE_CALENDAR_ICAL_URL is not set in the environment variables"
        )

    # Fetch the iCal data
    with urllib.request.urlopen(ical_url) as response:
        if response.status != 200:
            raise ValueError(f"HTTP error: {response.status}")
        ical_data = response.read().decode("utf-8")  # Read and decode the response

    # Parse the iCal data
    cal = Calendar.from_ical(ical_data)  # Use ical_data instead of response.text

    # Calculate the date range
    if days_ago is not None:
        now = date.today()
        time_ago = now - timedelta(days=days_ago)

    events: List[GCalEvent] = []
    for component in cal.walk():
        if component.name == "VEVENT":
            start = component.get("dtstart").dt
            end = component.get("dtend").dt if component.get("dtend") else None
            summary = str(component.get("summary"))

            if isinstance(start, datetime):
                logging.info(
                    f"Skipping {summary} because it's not a stay (start is not a simple date)"
                )
                continue

            if not end:
                logging.info(
                    f"Skipping {summary} because it's not a stay (end is not set)"
                )
                continue

            if isinstance(end, datetime):
                logging.info(
                    f"Skipping {summary} because it's not a stay (end is not a simple date)"
                )
                continue

            # Check if the event is within the specified date range
            if days_ago is None or (time_ago <= start <= now):
                events.append(
                    {
                        "summary": summary,
                        "start": start,
                        "end": end,
                        "description": str(component.get("description", "")),
                        "id": component.get("uid"),
                    }
                )

    return events


# Filter only events which are for a person staying at Laplace
# They usually have the format "PERSON at/à Laplace/La Place"
def filter_stay_events(events: List[GCalEvent]) -> List[GCalStay]:

    def guest_name_from_summary(summary: str) -> str:
        # Remove Laplace/La Place from end
        summary = re.sub(r"\b[lL]a ?[pP]lace$", "", summary)
        # Remove " at " or "à"
        summary = summary.replace(" at ", "").replace("à", "")
        return summary.strip()

    def is_stay(event: GCalEvent) -> bool:
        if not "laplace" in event["summary"].lower().replace(" ", ""):
            logging.info(
                f"Skipping {event['summary']} because it's not a stay (no Laplace in name)"
            )
            return False
        return True

    return [
        {
            "summary": event["summary"],
            "start": event["start"],
            "end": event["end"],
            "description": event["description"],
            # Remove Laplace/La Place and previous word
            "guest": guest_name_from_summary(event["summary"]),
            "id": event["id"],
        }
        for event in events
        if is_stay(event)
    ]


def get_existing_notion_stays() -> List[NotionStay]:
    notion = Client(auth=os.environ["NOTION_TOKEN"])
    database_id = os.environ["NOTION_GUEST_STAYS_DB_ID"]

    results = notion.databases.query(database_id=database_id)
    results = cast(Dict[str, Any], results)

    # example of a row in notion_stay.json
    typed_objects: List[NotionStay] = []

    for item in results["results"]:
        row_id = item["id"]
        properties = item["properties"]

        if "title" in properties["Name"]:
            name = " ".join(part["plain_text"] for part in properties["Name"]["title"])
        else:
            logging.warning(f"Notion: No name found for {row_id}")
            name = "Empty stay"

        date_property = properties.get("Date", {}).get("date", {})
        start_date = date_property.get("start", None)
        end_date = date_property.get("end", None)
        if not start_date or not end_date:
            logging.warning(f"Notion: No date found for {name}")
            continue

        if properties["Guest name"]["rollup"]["array"]:
            guest_name = properties["Guest name"]["rollup"]["array"][0]["title"][0][
                "plain_text"
            ]
        else:
            logging.warning(f"Notion: No guest name found for {name}")
            guest_name = "Unknown guest"

        if properties.get("GCal ID", {}).get("rich_text", [{}])[0]["plain_text"]:
            gcal_id = properties.get("GCal ID", {}).get("rich_text", [{}])[0][
                "plain_text"
            ]
        else:
            logging.warning(f"Notion: No GCal ID found for {name}")
            continue

        typed_object: NotionStay = {
            "id": row_id,
            "Paid": properties["Paid"]["checkbox"],
            "Start": datetime.fromisoformat(start_date).date(),
            "End": datetime.fromisoformat(end_date).date(),
            "Guest": guest_name,
            "Name": name,
            "GCalID": gcal_id,
        }

        typed_objects.append(typed_object)

    return typed_objects


def get_existing_notion_guests() -> Dict[str, NotionGuest]:
    notion = Client(auth=os.environ["NOTION_TOKEN"])
    database_id = os.environ["NOTION_GUEST_DB_ID"]

    results = notion.databases.query(database_id=database_id)
    results = cast(Dict[str, Any], results)

    guests: Dict[str, NotionGuest] = {}
    for item in results["results"]:
        row_id = item["id"]
        properties = item["properties"]

        if "title" in properties["Name"]:
            name = " ".join(part["plain_text"] for part in properties["Name"]["title"])
        else:
            logging.warning(f"Notion: No name found for {row_id}")
            continue

        first_name = name.split(" ")[0].lower()
        guests[first_name] = {
            "id": row_id,
            "name": name,
        }

    return guests


# find gcal stays that are not in notion
# if guest name is in both and dates are the same, consider them the same
# if a notion stay has the same date but not the same guest name, warn of ambiguity and consider different
def find_missing_gcal_stays(
    gcal_stays: List[GCalStay], notion_stays: List[NotionStay]
) -> List[GCalStay]:

    existing_gcal_ids = set([stay["GCalID"] for stay in notion_stays])

    # check if the gcal event is in the notion stayse
    missing_stays: List[GCalStay] = []
    for gcal_stay in gcal_stays:
        if gcal_stay["id"] not in existing_gcal_ids:
            missing_stays.append(gcal_stay)
        else:
            logging.info(f"GCal event {gcal_stay["summary"]} already in Notion")

    return missing_stays


# def find_missing_gcal_guests(
#     gcal_stays: List[GCalStay], notion_guests: Dict[str, NotionGuest]
# ) -> List[GCalStay]:

#     # check if the gcal event is in the notion stayse
#     missing_guests
#     for gcal_stay in gcal_stays:
#         if gcal_stay["id"] not in existing_gcal_ids:
#             missing_stays.append(gcal_stay)


def add_stay_to_notion(
    event: GCalStay, existing_guests: Dict[str, NotionGuest]
) -> None:
    notion = Client(auth=os.environ["NOTION_TOKEN"])
    database_id = os.environ["NOTION_GUEST_STAYS_DB_ID"]

    if event["guest"].lower() in existing_guests:
        notion_guest_id = existing_guests[event["guest"].lower()]["id"]
        logging.info(f"Notion: Guest {event['guest']} found in Notion")
    else:
        logging.info(f"Notion: Guest {event['guest']} not found in Notion")
        new_guest = add_guest_to_notion(event["guest"])
        notion_guest_id = new_guest["id"]
        existing_guests[event["guest"].lower()] = new_guest

    properties = {
        "Name": {"title": [{"text": {"content": event["summary"]}}]},
        "Date": {
            "date": {
                "start": event["start"].isoformat(),
                "end": (event["end"] - timedelta(days=1)).isoformat(),
            }
        },
        "GCal ID": {
            "rich_text": [{"text": {"content": event["id"]}}],
        },
    }

    if notion_guest_id:
        properties["Guest"] = {
            "relation": [{"id": notion_guest_id}],
        }

    logging.info(f"Notion: Adding stay {event['summary']} to database")
    notion.pages.create(
        parent={"database_id": database_id},
        properties=properties,
    )


def add_guest_to_notion(guest: str) -> NotionGuest:
    notion = Client(auth=os.environ["NOTION_TOKEN"])
    database_id = os.environ["NOTION_GUEST_DB_ID"]

    properties = {
        "Name": {"title": [{"text": {"content": guest}}]},
    }

    logging.info(f"Notion: Adding guest {guest} to database")
    page = notion.pages.create(
        parent={"database_id": database_id},
        properties=properties,
    )
    page = cast(Dict[str, Any], page)

    return {
        "id": page["id"],
        "name": guest,
    }


def main() -> None:
    existing_guests = get_existing_notion_guests()
    print(existing_guests)
    existing_stays = get_existing_notion_stays()
    events = get_calendar_events()
    gcal_stays = filter_stay_events(events)

    print(f"Found {len(gcal_stays)} stays in Google Calendar")
    print(f"Found {len(existing_stays)} stays in Notion")

    missing_stays = find_missing_gcal_stays(gcal_stays, existing_stays)
    print(f"Found {len(missing_stays)} missing stays")
    for stay in missing_stays:
        print(
            stay["summary"],
            stay["start"],
            stay["end"],
            "->",
            stay["guest"],
            f"({stay['id']})",
        )

    # test_event_to_add: GCalStay = {
    #     "summary": "Test event",
    #     "start": date.today(),
    #     "end": date.today() + timedelta(days=1),
    #     "description": "This is a test event",
    #     "guest": "Test guest",
    #     "id": "test_id",
    # }

    # add_stay_to_notion(test_event_to_add)

    count = 0
    for stay in missing_stays:
        count += 1
        add_stay_to_notion(stay, existing_guests)

    logging.info(f"Added {count} stays to Notion")


if __name__ == "__main__":
    main()

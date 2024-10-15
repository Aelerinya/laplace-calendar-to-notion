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
    end: date | None
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
    GCalID: Optional[str]


def get_calendar_events(days_ago: int = 30) -> List[GCalEvent]:
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

            if end and isinstance(end, datetime):
                logging.info(
                    f"Skipping {summary} because it's not a stay (end is not a simple date)"
                )
                continue

            # Check if the event is within the specified date range
            if time_ago <= start <= now:
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
            "end": event["end"] if event["end"] else event["start"],
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

    # save to json
    with open("notion_stays.json", "w") as f:
        json.dump(results["results"][0], f, indent=4)

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

        typed_object: NotionStay = {
            "id": row_id,
            "Paid": properties["Paid"]["checkbox"],
            "Start": datetime.fromisoformat(start_date).date(),
            "End": datetime.fromisoformat(end_date).date(),
            "Guest": guest_name,
            "Name": name,
            "GCalID": properties.get("GCal ID", {}).get("rich_text", [{}])[0][
                "plain_text"
            ],
        }

        typed_objects.append(typed_object)

    return typed_objects


# find gcal stays that are not in notion
# if guest name is in both and dates are the same, consider them the same
# if a notion stay has the same date but not the same guest name, warn of ambiguity and consider different
def find_missing_gcal_stays(
    gcal_stays: List[GCalStay], notion_stays: List[NotionStay]
) -> List[GCalStay]:
    missing_stays: List[GCalStay] = []
    for gcal_stay in gcal_stays:
        for notion_stay in notion_stays:
            print(" ==== Comparing ==== ")
            print(gcal_stay["start"], notion_stay["Start"])
            print(gcal_stay["end"], notion_stay["End"])
            print(gcal_stay["guest"], notion_stay["Guest"])

            if (
                gcal_stay["start"] == notion_stay["Start"]
                # Notion end date is inclusive, but GCal is exclusive, so add one day
                and gcal_stay["end"] == notion_stay["End"] + timedelta(days=1)
            ):
                first_name = gcal_stay["guest"].split(" ")[0].lower()
                if first_name in notion_stay["Guest"].lower() or (
                    notion_stay["Name"] and first_name in notion_stay["Name"].lower()
                ):
                    print(f"Found: {gcal_stay['summary']} on {gcal_stay['start']}")
                    break  # Exit the inner loop if a match is found
                else:
                    print(
                        f"Warning: Ambiguity between '{gcal_stay['summary']}' and '{notion_stay['Name']}' on {gcal_stay['start']}"
                    )
        else:  # This else executes if the inner loop did not break
            missing_stays.append(gcal_stay)

    return missing_stays


def add_to_notion(event: GCalStay) -> None:
    notion = Client(auth=os.environ["NOTION_TOKEN"])
    database_id = os.environ["NOTION_GUEST_STAYS_DB_ID"]

    notion.pages.create(
        parent={"database_id": database_id},
        properties={
            "Name": {"title": [{"text": {"content": event["summary"]}}]},
            "Date": {
                "date": {
                    "start": event["start"].isoformat(),
                    "end": event["end"].isoformat(),
                }
            },
            "Guest": {"relation": [{"id": "120baaa5-2195-8126-b025-c8ab1dc374d1"}]},
            # "Guest name": {
            #     "rollup": {
            #         "array": [{"title": [{"text": {"content": event["guest"]}}]}]
            #     }
            # },
        },
    )


def main() -> None:
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
    # }

    # add_to_notion(test_event_to_add)


if __name__ == "__main__":
    main()

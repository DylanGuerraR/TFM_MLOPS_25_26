import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer


def weighted_choice(weight_map: dict):
    """
    weight_map: {"value": weight, ...}
    returns one key based on weights
    """
    items = list(weight_map.items())
    values = [v for v, _ in items]
    weights = [w for _, w in items]
    return random.choices(values, weights=weights, k=1)[0]


def env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    return int(v) if v else default


def env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    return float(v) if v else default


def env_str(name: str, default: str) -> str:
    v = os.getenv(name, "").strip()
    return v if v else default


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


# ----------------------------
# Main producer
# ----------------------------
def main():
    # ---- Config (env) ----
    bootstrap = env_str(
        "KAFKA_BOOTSTRAP_SERVERS",
        "broker-1:29092,broker-2:29093,broker-3:29094",
    )
    topic = env_str("KAFKA_TOPIC", "leads_raw")

    rows = env_int("ROWS", 1000)

    # simulate a month by default
    days = env_int("DAYS", 30)
    start_date_str = os.getenv("START_DATE", "").strip()  # YYYY-MM-DD
    if start_date_str:
        start_dt = datetime.fromisoformat(start_date_str).replace(tzinfo=timezone.utc)
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=days)

    seed = env_int("SEED", 42)
    convert_rate = env_float("CONVERT_RATE", 0.35)  # tweak later if you want
    sleep_ms = env_int("SLEEP_MS", 0)  # 0 = as fast as possible

    # For traceability
    schema_version = env_str("SCHEMA_VERSION", "v1")
    source_system = env_str("SOURCE_SYSTEM", "kaggle_csv_synthetic")

    random.seed(seed)

    # Faker locale: India-like (fits the dataset context)
    fake = Faker("en_IN")

    # ---- Realistic categorical distributions (from your EDA notebook) ----
    # City (from value_counts in notebook)
    CITY_W = {
        "Mumbai": 3016,
        "Thane & Outskirts": 670,
        "Other Cities": 625,
        "Unknown": 607,
        "Other Cities of Maharashtra": 416,
        "Other Metro Cities": 369,
        "Select": 228,
        "Tier II Cities": 73,
    }

    LEAD_ORIGIN_W = {
        "Landing Page Submission": 4808,
        "API": 1091,
        "Lead Add Form": 96,
        "Lead Import": 9,
    }

    LEAD_SOURCE_W = {
        "Direct Traffic": 2454,
        "Google": 2321,
        "Organic Search": 906,
        "Olark Chat": 126,
        "Reference": 89,
        "Referral Sites": 76,
        "Facebook": 9,
        "bing": 5,
        "Welingak Website": 5,
        "Click2call": 2,
        "Social Media": 2,
        "Press_Release": 2,
        "welearnblog_Home": 1,
        "youtubechannel": 1,
        "WeLearn": 1,
        "testone": 1,
        "Pay per Click Ads": 1,
        "blog": 1,
        "NC_EDM": 1,
    }

    LAST_ACTIVITY_W = {
        "Email Opened": 2353,
        "SMS Sent": 1844,
        "Page Visited on Website": 550,
        "Converted to Lead": 345,
        "Email Bounced": 235,
        "Olark Chat Conversation": 212,
        "Email Link Clicked": 148,
        "Form Submitted on Website": 113,
        "Unreachable": 71,
        "Unknown": 49,
        "Unsubscribed": 49,
        "Had a Phone Conversation": 24,
        "View in browser link Clicked": 4,
        "Approached upfront": 3,
        "Email Received": 2,
        "Visited Booth in Tradeshow": 1,
        "Email Marked Spam": 1,
    }

    SPECIALIZATION_W = {
        "Finance Management": 894,
        "Human Resource Management": 760,
        "Marketing Management": 731,
        "Unknown": 624,
        "Operations Management": 467,
        "Business Administration": 373,
        "IT Projects Management": 354,
        "Supply Chain Management": 328,
        "Banking, Investment And Insurance": 304,
        "Travel and Tourism": 199,
        "Media and Advertising": 193,
        "International Business": 170,
        "Healthcare Management": 139,
        "E-COMMERCE": 106,
        "Hospitality Management": 102,
        "Retail Management": 98,
        "Rural and Agribusiness": 68,
        "E-Business": 56,
        "Services Excellence": 38,
    }

    OCCUPATION_W = {
        "Unemployed": 3504,
        "Unknown": 1863,
        "Working Professional": 491,
        "Student": 119,
        "Other": 14,
        "Housewife": 8,
        "Businessman": 5,
    }

    TAGS_W = {
        "Unknown": 2014,
        "Will revert after reading the email": 1445,
        "Ringing": 864,
        "Already a student": 283,
        "Interested in other courses": 268,
        # the rest exists but was truncated in the notebook output;
        # we add some generic buckets to keep variety without overfitting:
        "Busy": 180,
        "Not Interested": 220,
        "Invalid Number": 140,
        "Lost to EINS": 80,
        "Closed by Horizzon": 60,
        "Other": 200,
    }

    LAST_NOTABLE_W = {
        "Modified": 2043,
        "Email Opened": 1987,
        "SMS Sent": 1436,
        "Page Visited on Website": 271,
        "Email Link Clicked": 100,
        "Olark Chat Conversation": 52,
        "Unsubscribed": 38,
        "Email Bounced": 35,
        "Unreachable": 24,
        "Had a Phone Conversation": 13,
        "Form Submitted on Website": 1,
        "Email Received": 1,
        "Approached upfront": 1,
        "View in browser link Clicked": 1,
        "Email Marked Spam": 1,
    }

    # Country in notebook shows India dominant; full list was long.
    # We'll keep a realistic small set + Unknown + Other.
    COUNTRY_W = {
        "India": 5594,
        "Unknown": 153,
        "United States": 61,
        "United Arab Emirates": 47,
        "Saudi Arabia": 20,
        "Singapore": 12,
        "United Kingdom": 10,
        "Australia": 8,
        "Other": 30,
    }

    # Asymmetrique Index values are usually "01.Low" / "02.Medium" / "03.High"
    ASYM_INDEX_W = {"01.Low": 45, "02.Medium": 35, "03.High": 20}

    # ---- Kafka producer ----
    producer = KafkaProducer(
        bootstrap_servers=[b.strip() for b in bootstrap.split(",")],
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=8,
        linger_ms=10,
    )

    # ---- Generate & send ----
    flush_every = env_int("FLUSH_EVERY", 2000)

    for i in range(rows):
        # event time inside the window
        event_time = start_dt + timedelta(seconds=random.randint(0, days * 24 * 3600))
        ingestion_time = datetime.now(timezone.utc)

        # core identifiers
        prospect_id = fake.uuid4()
        lead_number = random.randint(1000000, 9999999)

        # numerics - keep coherent types and constraints
        total_visits = clamp(int(random.gauss(5, 4)), 0, 60)
        total_time = clamp(int(random.gauss(450, 350)), 1, 5000)  # >0
        page_views = round(clamp(random.gauss(2.5, 1.2), 0.1, 20.0), 2)

        # binary target
        converted = 1 if random.random() < convert_rate else 0

        # realistic categorical
        city = weighted_choice(CITY_W)
        country = weighted_choice(COUNTRY_W)
        if country == "Other":
            # random but stable-like: keep it as string
            country = random.choice(
                ["Canada", "Qatar", "South Africa", "Nigeria", "Kenya", "Germany", "France"]
            )

        msg = {
            # ---- Metadata (not in original Kaggle columns but very useful) ----
            "event_time": iso_utc(event_time),
            "ingestion_time": iso_utc(ingestion_time),
            "source_system": source_system,
            "schema_version": schema_version,

            # ---- RAW Kaggle columns (as you listed) ----
            "Prospect ID": prospect_id,
            "Lead Number": lead_number,
            "Lead Origin": weighted_choice(LEAD_ORIGIN_W),
            "Lead Source": weighted_choice(LEAD_SOURCE_W),
            "Do Not Email": random.choices(["Yes", "No"], weights=[8, 92], k=1)[0],
            "Do Not Call": random.choices(["Yes", "No"], weights=[2, 98], k=1)[0],
            "Converted": converted,
            "TotalVisits": total_visits,
            "Total Time Spent on Website": total_time,
            "Page Views Per Visit": page_views,
            "Last Activity": weighted_choice(LAST_ACTIVITY_W),
            "Country": country,
            "Specialization": weighted_choice(SPECIALIZATION_W),
            "How did you hear about X Education": random.choice(
                ["Online Search", "Word of Mouth", "Social Media", "Advertisements", "Email", "Unknown"]
            ),  # will likely be dropped later
            "What is your current occupation": weighted_choice(OCCUPATION_W),
            "What matters most to you in choosing a course": random.choice(
                ["Better Career Prospects", "Flexibility", "Reputation", "Fees", "Unknown"]
            ),  # will likely be dropped later

            # the following are mostly dummy/rare flags (often dropped later)
            "Search": random.choices(["Yes", "No"], weights=[3, 97], k=1)[0],
            "Magazine": random.choices(["Yes", "No"], weights=[1, 99], k=1)[0],
            "Newspaper Article": random.choices(["Yes", "No"], weights=[1, 99], k=1)[0],
            "X Education Forums": random.choices(["Yes", "No"], weights=[1, 99], k=1)[0],
            "Newspaper": random.choices(["Yes", "No"], weights=[1, 99], k=1)[0],
            "Digital Advertisement": random.choices(["Yes", "No"], weights=[2, 98], k=1)[0],
            "Through Recommendations": random.choices(["Yes", "No"], weights=[2, 98], k=1)[0],
            "Receive More Updates About Our Courses": random.choices(["Yes", "No"], weights=[5, 95], k=1)[0],
            "Tags": weighted_choice(TAGS_W),
            "Lead Quality": random.choice(["Low in Relevance", "Might be", "Not Sure", "High in Relevance", "Unknown"]),
            "Update me on Supply Chain Content": random.choices(["Yes", "No"], weights=[4, 96], k=1)[0],
            "Get updates on DM Content": random.choices(["Yes", "No"], weights=[4, 96], k=1)[0],
            "Lead Profile": random.choice(["Potential Lead", "Select", "Other Leads", "Unknown"]),
            "City": city,
            "Asymmetrique Activity Index": weighted_choice(ASYM_INDEX_W),
            "Asymmetrique Profile Index": weighted_choice(ASYM_INDEX_W),
            "Asymmetrique Activity Score": clamp(int(random.gauss(14, 6)), 0, 30),
            "Asymmetrique Profile Score": clamp(int(random.gauss(14, 6)), 0, 30),
            "I agree to pay the amount through cheque": random.choices(["Yes", "No"], weights=[2, 98], k=1)[0],
            "A free copy of Mastering The Interview": random.choices(["True", "False"], weights=[48, 52], k=1)[0],
            "Last Notable Activity": weighted_choice(LAST_NOTABLE_W),
        }

        # send message (key = lead_number)
        producer.send(topic, key=lead_number, value=msg)

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

        if (i + 1) % flush_every == 0:
            producer.flush()
            print(f"[producer] sent {i+1}/{rows}")

    producer.flush()
    producer.close()
    print(f"[producer] DONE. Produced {rows} messages to topic '{topic}'")


if __name__ == "__main__":
    main()

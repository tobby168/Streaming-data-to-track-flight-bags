from datetime import timedelta

# Airport + schedule sources
AIRPORT = "TPE"
SCHEDULE_RAW_PATH = "data/tpe_schedule_raw.json"
SCHEDULE_DAYS = 7
# Only emit flights in a rolling window around "now"
ACTIVE_WINDOW_PAST_MINUTES = 60
ACTIVE_WINDOW_FUTURE_MINUTES = 180
MAX_FLIGHTS_PER_BATCH = 60
EMIT_LOOKAHEAD_SECONDS = 30

# Transfer behavior (kept low for TPE, mostly O&D)
TRANSFER_RATIO = 0.05
TRANSFER_P90_MINUTES = 35
BUFFER_MINUTES = 10

# Aircraft seating defaults (approximate)
PASSENGER_CAPACITY_DEFAULT = 190
AIRCRAFT_CAPACITY = {
    "A20N": 188,
    "A321": 200,
    "A21N": 200,
    "A320": 180,
    "A333": 277,
    "A332": 250,
    "A359": 320,
    "A388": 500,
    "B738": 186,
    "B739": 210,
    "B744": 416,
    "B748": 467,
    "B752": 200,
    "B763": 261,
    "B772": 314,
    "B77W": 370,
    "B788": 254,
    "B789": 290,
    "B78X": 330,
    "E190": 110,
}

# Load factor ranges by route class (mean, std, min, max)
LOAD_FACTOR = {
    "short": (0.82, 0.06, 0.65, 0.97),
    "medium": (0.84, 0.05, 0.68, 0.98),
    "long": (0.88, 0.04, 0.75, 0.99),
}
# Cap per-flight passenger counts to avoid runaway volumes
MAX_PASSENGERS_PER_FLIGHT = 220

# Passenger group sizing (probability for group sizes 1-4)
GROUP_SIZE_WEIGHTS = [0.62, 0.24, 0.09, 0.05]

# Bag count weights per passenger by route class for 0,1,2,3 bags
BAG_COUNT_WEIGHTS_BY_ROUTE = {
    "short": [0.20, 0.55, 0.22, 0.03],
    "medium": [0.12, 0.50, 0.32, 0.06],
    "long": [0.05, 0.55, 0.35, 0.05],
}

# Out-of-order ingest
OUT_OF_ORDER_RATE = 0.05
OUT_OF_ORDER_MIN_DELAY = timedelta(minutes=2)
OUT_OF_ORDER_MAX_DELAY = timedelta(minutes=6)
DEFAULT_BAG_CUTOFF_OFFSET = timedelta(minutes=45)

# Passenger arrival curve (minutes before departure)
CHECKIN_P50 = {"short": 115, "medium": 130, "long": 160}
CHECKIN_P90 = {"short": 180, "medium": 210, "long": 260}
CHECKIN_MIN = 45
CHECKIN_MAX = 320

# Event timing distributions (minutes)
INDUCTED_P50 = 6
INDUCTED_P90 = 16
SCREENED_P50 = 3
SCREENED_P90 = 10
MAKEUP_P50 = 10
MAKEUP_P90 = 25
LOADED_BUFFER_MINUTES = 8

# Route classification helpers
LONG_HAUL_DESTS = {"LAX", "SFO", "YVR", "YYZ", "LHR", "CDG", "AMS", "FRA", "SEA"}
MEDIUM_HAUL_DESTS = {"SIN", "BKK", "KUL", "MNL", "SGN", "DEL", "BOM", "SYD"}

# Block time fallback (minutes)
BLOCK_TIME_BY_DEST = {
    "HKG": 95,
    "NRT": 170,
    "HND": 170,
    "KIX": 165,
    "BKK": 215,
    "MNL": 130,
    "PVG": 135,
    "ICN": 155,
    "OKA": 85,
    "PUS": 150,
    "CTS": 210,
    "SIN": 270,
    "SGN": 200,
    "KUL": 230,
    "MFM": 95,
    "CEB": 175,
}
BLOCK_TIME_BY_CLASS = {"short": 140, "medium": 240, "long": 520}

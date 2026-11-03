"""Geospatial helpers shared by Spark jobs, the producer and the dashboard."""
from __future__ import annotations

import math

import h3
import numpy as np

EARTH_RADIUS_KM = 6371.0088

# Approximate neighbourhood / landmark centroids, used only to give hex cells and
# alerts a human-readable label ("near Times Square").
PLACES = {
    "nyc": [
        ("Financial District", 40.7075, -74.0113), ("Tribeca", 40.7163, -74.0086),
        ("SoHo", 40.7233, -74.0030), ("Chinatown", 40.7158, -73.9970),
        ("Lower East Side", 40.7150, -73.9843), ("Greenwich Village", 40.7336, -74.0027),
        ("East Village", 40.7265, -73.9815), ("Chelsea", 40.7465, -74.0014),
        ("Union Square", 40.7359, -73.9911), ("Flatiron", 40.7411, -73.9897),
        ("Times Square", 40.7580, -73.9855), ("Penn Station / MSG", 40.7505, -73.9934),
        ("Grand Central", 40.7527, -73.9772), ("Hell's Kitchen", 40.7638, -73.9918),
        ("Upper West Side", 40.7870, -73.9754), ("Upper East Side", 40.7736, -73.9566),
        ("Central Park", 40.7812, -73.9665), ("Harlem", 40.8116, -73.9465),
        ("Washington Heights", 40.8417, -73.9394), ("Williamsburg", 40.7081, -73.9571),
        ("Greenpoint", 40.7305, -73.9515), ("Downtown Brooklyn", 40.6928, -73.9903),
        ("DUMBO", 40.7033, -73.9881), ("Park Slope", 40.6710, -73.9814),
        ("Bushwick", 40.6944, -73.9213), ("Bed-Stuy", 40.6872, -73.9418),
        ("Crown Heights", 40.6694, -73.9422), ("Flatbush", 40.6409, -73.9624),
        ("Sunset Park", 40.6455, -74.0124), ("Bay Ridge", 40.6264, -74.0299),
        ("Coney Island", 40.5755, -73.9707), ("Long Island City", 40.7447, -73.9485),
        ("Astoria", 40.7644, -73.9235), ("Jackson Heights", 40.7557, -73.8831),
        ("Flushing", 40.7675, -73.8331), ("Forest Hills", 40.7181, -73.8448),
        ("Ridgewood", 40.7043, -73.9018), ("Jamaica", 40.7027, -73.7890),
        ("JFK Airport", 40.6413, -73.7781), ("LaGuardia Airport", 40.7769, -73.8740),
        ("Yankee Stadium", 40.8296, -73.9262), ("Fordham", 40.8615, -73.8905),
        ("St. George (Staten Is.)", 40.6437, -74.0739), ("Jersey City", 40.7178, -74.0431),
        ("Hoboken", 40.7440, -74.0324), ("Newark", 40.7357, -74.1724),
    ],
    "tky": [
        ("Shinjuku", 35.6909, 139.7003), ("Shibuya", 35.6580, 139.7016),
        ("Harajuku", 35.6702, 139.7027), ("Roppongi", 35.6628, 139.7314),
        ("Ginza", 35.6717, 139.7650), ("Tokyo Station", 35.6812, 139.7671),
        ("Akihabara", 35.6984, 139.7731), ("Kanda", 35.6918, 139.7709),
        ("Ueno", 35.7138, 139.7773), ("Asakusa", 35.7148, 139.7967),
        ("Ikebukuro", 35.7295, 139.7109), ("Shinagawa", 35.6285, 139.7388),
        ("Odaiba", 35.6299, 139.7765), ("Ebisu", 35.6467, 139.7101),
        ("Meguro", 35.6339, 139.7157), ("Nakameguro", 35.6440, 139.6988),
        ("Shimokitazawa", 35.6618, 139.6680), ("Sangenjaya", 35.6436, 139.6714),
        ("Jiyugaoka", 35.6077, 139.6688), ("Futako-Tamagawa", 35.6116, 139.6268),
        ("Kichijoji", 35.7030, 139.5795), ("Nakano", 35.7058, 139.6658),
        ("Koenji", 35.7052, 139.6496), ("Ochanomizu", 35.6993, 139.7650),
        ("Iidabashi", 35.7020, 139.7450), ("Kudanshita", 35.6954, 139.7514),
        ("Shimbashi", 35.6663, 139.7583), ("Hamamatsucho", 35.6553, 139.7570),
        ("Tamachi", 35.6457, 139.7476), ("Gotanda", 35.6262, 139.7236),
        ("Osaki", 35.6197, 139.7286), ("Kamata", 35.5625, 139.7160),
        ("Haneda Airport", 35.5494, 139.7798), ("Ryogoku", 35.6967, 139.7929),
        ("Kinshicho", 35.6969, 139.8146), ("Tokyo Skytree", 35.7101, 139.8107),
        ("Kita-Senju", 35.7493, 139.8048), ("Nippori", 35.7280, 139.7709),
        ("Akabane", 35.7779, 139.7210), ("Itabashi", 35.7512, 139.7090),
        ("Nerima", 35.7378, 139.6541), ("Toyosu", 35.6550, 139.7962),
        ("Kasai", 35.6640, 139.8730), ("Tokyo Disney Resort", 35.6329, 139.8804),
        ("Funabashi", 35.7016, 139.9853), ("Tachikawa", 35.6979, 139.4139),
        ("Machida", 35.5420, 139.4455), ("Musashi-Kosugi", 35.5765, 139.6596),
        ("Kawasaki", 35.5313, 139.6969), ("Yokohama", 35.4658, 139.6223),
        ("Omiya", 35.9064, 139.6237),
    ],
}

# Rough continental bounding boxes for the global (Gowalla) view, checked in order.
REGIONS = [
    ("North America", 15, 75, -170, -50),
    ("South America", -60, 15, -95, -30),
    ("Europe", 35, 72, -25, 45),
    ("Africa", -40, 35, -20, 55),
    ("Oceania", -50, -10, 110, 180),
    ("Asia", -10, 75, 45, 180),
]


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km; works on scalars and numpy arrays (broadcasting)."""
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def latlng_to_cell(lat: float, lon: float, res: int) -> str | None:
    if lat is None or lon is None or math.isnan(lat) or math.isnan(lon):
        return None
    return h3.latlng_to_cell(lat, lon, res)


def cell_center(cell: str) -> tuple[float, float]:
    return h3.cell_to_latlng(cell)


def nearest_place(dataset: str, lat: float, lon: float, max_km: float = 3.0) -> str | None:
    """Name of the closest known landmark within max_km, else None."""
    places = PLACES.get(dataset)
    if not places:
        return None
    names = [p[0] for p in places]
    d = haversine_km(lat, lon, [p[1] for p in places], [p[2] for p in places])
    i = int(np.argmin(d))
    return names[i] if d[i] <= max_km else None


def describe_location(dataset: str, lat: float, lon: float) -> str:
    place = nearest_place(dataset, lat, lon)
    return f"near {place}" if place else f"{lat:.3f}, {lon:.3f}"


def region_of(lat: float, lon: float) -> str:
    for name, lat0, lat1, lon0, lon1 in REGIONS:
        if lat0 <= lat <= lat1 and lon0 <= lon <= lon1:
            return name
    return "Other"


def region_case_sql(lat_col: str = "lat", lon_col: str = "lon") -> str:
    """The same classification as region_of(), as a Spark/Hive SQL CASE expression."""
    whens = " ".join(
        f"WHEN {lat_col} BETWEEN {a} AND {b} AND {lon_col} BETWEEN {c} AND {d} THEN '{n}'"
        for n, a, b, c, d in REGIONS
    )
    return f"CASE {whens} ELSE 'Other' END"

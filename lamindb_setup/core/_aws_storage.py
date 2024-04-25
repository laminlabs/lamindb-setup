from __future__ import annotations


def get_location(ip="ipinfo.io"):
    import requests  # type: ignore

    response = requests.get(f"http://{ip}/json").json()
    loc = response["loc"].split(",")
    return {"latitude": float(loc[0]), "longitude": float(loc[1])}


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    import math

    R = 6371  # Radius of the Earth in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(
        math.radians(lat1)
    ) * math.cos(math.radians(lat2)) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance


def find_closest_aws_region() -> str:
    aws_region_locations = {
        "us-east-1": {"latitude": 39.0, "longitude": -77.0},  # Northern Virginia
        "us-east-2": {"latitude": 40.0, "longitude": -83.0},  # Ohio
        "us-west-1": {"latitude": 37.77, "longitude": -122.41},  # Northern California
        "us-west-2": {"latitude": 45.52, "longitude": -122.68},  # Oregon
        "eu-central-1": {"latitude": 50.11, "longitude": 8.68},  # Frankfurt
        "eu-west-2": {"latitude": 51.51, "longitude": -0.13},  # London, UK
    }
    your_location = get_location()
    closest_region = ""
    min_distance = float("inf")
    for region in aws_region_locations:
        region_location = aws_region_locations[region]
        distance = haversine(
            your_location["latitude"],
            your_location["longitude"],
            region_location["latitude"],
            region_location["longitude"],
        )
        if distance < min_distance:
            closest_region, min_distance = region, distance
    return closest_region

import requests  # type: ignore
import math
import botocore.session


def get_location(ip="ipinfo.io"):
    response = requests.get(f"http://{ip}/json").json()
    loc = response["loc"].split(",")
    return {"latitude": float(loc[0]), "longitude": float(loc[1])}


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371  # Radius of the Earth in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat / 2) * math.sin(dLat / 2) + math.cos(
        math.radians(lat1)
    ) * math.cos(math.radians(lat2)) * math.sin(dLon / 2) * math.sin(dLon / 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance


def get_aws_regions(service_name="s3") -> str:
    session = botocore.session.get_session()
    s3 = session.create_client(service_name)
    return s3.meta.region_name


def find_closest_aws_region():
    your_location = get_location()
    aws_regions = get_aws_regions()
    closest_region = None
    min_distance = float("inf")
    for region in aws_regions:
        # Approximate latitudes and longitudes for AWS regions
        region_locations = {
            # 'us-east-1': {'latitude': 39.0, 'longitude': -77.0},  # Northern Virginia
            "us-west-2": {"latitude": 45.52, "longitude": -122.68},  # Oregon
            "eu-central-1": {
                "latitude": 50.11,
                "longitude": 8.68,
            },  # Frankfurt, Germany
            # Add other regions here
        }
        if region in region_locations:
            region_location = region_locations[region]
            distance = haversine(
                your_location["latitude"],
                your_location["longitude"],
                region_location["latitude"],
                region_location["longitude"],
            )
            if distance < min_distance:
                min_distance = distance
                closest_region = region
    return closest_region

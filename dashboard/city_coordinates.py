"""
City coordinates configuration for geographic visualization.

Coordinates are approximate city-centre locations sourced from public
geographic reference data. These are NOT precise sensor locations -
they represent the general area of each city.

All coordinates are labelled as approximate throughout the dashboard.
"""

# Approximate city-centre coordinates (latitude, longitude)
CITY_COORDINATES = {
    "Amritsar": {"lat": 31.6340, "lon": 74.8723, "country": "India"},
    "Ludhiana": {"lat": 30.9010, "lon": 75.8573, "country": "India"},
    "Jalandhar": {"lat": 31.3260, "lon": 75.5762, "country": "India"},
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "country": "India"},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777, "country": "India"},
    "Chennai": {"lat": 13.0827, "lon": 80.2707, "country": "India"},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639, "country": "India"},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946, "country": "India"},
    "Bangalore": {"lat": 12.9716, "lon": 77.5946, "country": "India"},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867, "country": "India"},
    "Pune": {"lat": 18.5204, "lon": 73.8567, "country": "India"},
}

CANONICAL_CITIES = [
    "Amritsar",
    "Bengaluru",
    "Chennai",
    "Delhi",
    "Hyderabad",
    "Jalandhar",
    "Kolkata",
    "Ludhiana",
    "Mumbai",
    "Pune",
]

CITIES = CANONICAL_CITIES


def get_city_coordinates(city: str) -> dict | None:
    """Return coordinates for a city, or None if not available."""
    canonical = "Bengaluru" if city == "Bangalore" else city
    return CITY_COORDINATES.get(canonical) or CITY_COORDINATES.get(city)


def get_cities_with_coordinates() -> list[str]:
    """Return list of canonical cities that have known coordinates."""
    return sorted(CANONICAL_CITIES)

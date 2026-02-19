INDIA_BBOX = (6.0, 37.0, 68.0, 97.0)
min_lat, max_lat, min_lon, max_lon = INDIA_BBOX
print(
        f"https://opensky-network.org/api/states/all"
        f"?lamin={min_lat}&lamax={max_lat}&lomin={min_lon}&lomax={max_lon}"
    )
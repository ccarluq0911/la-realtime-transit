def gtfs_time_to_seconds_str(time_str):
    """Convierte 'HH:MM:SS' a segundos desde medianoche (soporta horas >24)."""
    parts = time_str.split(":")
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])

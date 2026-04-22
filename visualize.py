"""
visualize.py
Generates an interactive HTML map of the France 0.18° weather grid (~20 km).
Shows training grid points, test-zone cities, and their proportional exclusion rings.

Requirements: pip install folium
Output: france_grid.html  (open in any browser for presentation)
"""

import folium


LAT_START, LAT_END, LAT_STEP = 42.0, 51.25, 0.18
LON_START, LON_END, LON_STEP = -5.0,  8.25, 0.18


def france_grid() -> list[tuple[float, float]]:
    points, lat = [], LAT_START
    while lat <= LAT_END + 1e-9:
        lon = LON_START
        while lon <= LON_END + 1e-9:
            points.append((round(lat, 2), round(lon, 2)))
            lon += LON_STEP
        lat += LAT_STEP
    return points



TEST_ZONES = [
    {"name": "Paris",      "lat": 48.857, "lon":  2.352, "radius_km": 40},
    {"name": "Lyon",       "lat": 45.764, "lon":  4.836, "radius_km": 28},
    {"name": "Grenoble",   "lat": 45.188, "lon":  5.724, "radius_km": 15},
    {"name": "Toulouse",   "lat": 43.605, "lon":  1.444, "radius_km": 24},
    {"name": "Bordeaux",   "lat": 44.838, "lon": -0.579, "radius_km": 24},
    {"name": "Lille",      "lat": 50.629, "lon":  3.057, "radius_km": 20},
    {"name": "Nantes",     "lat": 47.218, "lon": -1.554, "radius_km": 20},
    {"name": "Rennes",     "lat": 48.117, "lon": -1.678, "radius_km": 16},
    {"name": "Strasbourg", "lat": 48.573, "lon":  7.752, "radius_km": 16},
    {"name": "Avignon",    "lat": 43.949, "lon":  4.805, "radius_km": 24},
]



def build_map() -> folium.Map:
    m = folium.Map(
        location=[46.5, 2.5],
        zoom_start=6,
        tiles="CartoDB positron",  
    )

    legend_html = """
    <div style="position:fixed; bottom:30px; left:30px; z-index:1000;
                background:white; padding:12px 16px; border-radius:8px;
                border:1px solid #ccc; font-family:sans-serif; font-size:13px;">
        <b>France Weather Grid</b><br><br>
        <span style="color:#3186cc;">&#9679;</span> Training grid point (0.18°, ~20 km)<br>
        <span style="color:#e74c3c;">&#9679;</span> Test zone city (withheld)<br>
        <span style="color:#e74c3c; opacity:0.4;">&#11044;</span> Proportional exclusion ring
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    grid_layer = folium.FeatureGroup(name="Training grid points")
    for lat, lon in france_grid():
        folium.CircleMarker(
            location=[lat, lon],
            radius=3,
            color="#3186cc",
            fill=True,
            fill_color="#3186cc",
            fill_opacity=0.7,
            weight=0,
            tooltip=f"({lat}, {lon})",
        ).add_to(grid_layer)
    grid_layer.add_to(m)

    zone_layer = folium.FeatureGroup(name="Test zones (withheld cities)")
    for city in TEST_ZONES:
        radius_m = city["radius_km"] * 1000
        folium.Circle(
            location=[city["lat"], city["lon"]],
            radius=radius_m,
            color="#e74c3c",
            fill=True,
            fill_color="#e74c3c",
            fill_opacity=0.12,
            weight=1.5,
        ).add_to(zone_layer)

        folium.CircleMarker(
            location=[city["lat"], city["lon"]],
            radius=6,
            color="#c0392b",
            fill=True,
            fill_color="#e74c3c",
            fill_opacity=1.0,
            weight=2,
            tooltip=f"<b>{city['name']}</b><br>Test zone — withheld from training",
            popup=folium.Popup(
                f"<b>{city['name']}</b><br>"
                f"Lat: {city['lat']}  Lon: {city['lon']}<br>"
                f"Exclusion radius: {city['radius_km']} km",
                max_width=200,
            ),
        ).add_to(zone_layer)

    zone_layer.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    return m



if __name__ == "__main__":
    output = "france_grid.html"
    m = build_map()
    m.save(output)
    print(f"Map saved to {output} — open in your browser for the presentation.")

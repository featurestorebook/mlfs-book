import os

img_dir = "docs/air-quality/assets/img"
sensors = [d for d in os.listdir(img_dir) if os.path.isdir(os.path.join(img_dir, d))]
sensors.sort()

dashboard_md = "# Air Quality Dashboard\n\n"

# Include logo at the top
dashboard_md += "![Hopsworks Logo](../titanic/assets/img/logo.png)\n\n"

for sensor in sensors:
    dashboard_md += f"## Sensor: {sensor}\n\n"
    dashboard_md += f"![Forecast](./assets/img/{sensor}/pm25_forecast.png)\n\n"
    dashboard_md += f"![Hindcast](./assets/img/{sensor}/pm25_hindcast_1day.png)\n\n"

dashboard_file = "docs/air-quality/index.md"
with open(dashboard_file, "w") as f:
    f.write(dashboard_md)

import sys
import argparse
import requests

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--url", help="Target endpoint (…/api/v1/tree)")
    p.add_argument("--user", help="Basic auth user", default=None)
    p.add_argument("--password", help="Basic auth password", default=None)
    args = p.parse_args()

    url = args.url
    if not url:
        print("ERROR: Missing --url")
        return 2

    user = args.user
    pwd  = args.password
    auth = (user, pwd) if user and pwd else None

    headers = {"Content-Type": "text/csv"}

    sensor_data = """timestamp,element,tn_voigt_3d,pressure,conductivity_tn
      2025-05-13T08:00:00,1,0,1.23,0.00001
      2025-05-13T08:00:00,1,1,1.23,0.00002
      2025-05-13T08:00:00,1,2,1.23,0.00003
      """

    tree_data = """
      time,temperature
      2025-05-13T08:00:00,20.0
      2025-05-13T09:00:00,21.5
      """

    weather_data = """
      timestamp,latitude,longitude,temp
      2025-05-13T07:00:00Z,10.0,10.0,6.85
      2025-05-13T07:00:00Z,20.0,10.0,7.85
      2025-05-13T07:00:00Z,20.0,20.0,8.85
      """

    def post_csv(data, endpoint):
        return requests.post(f"{url}/{endpoint}", headers=headers, data=data.encode("utf-8"), auth=auth)

    sensor_response = post_csv(sensor_data, "api/v1/sensor")
    tree_response = post_csv(tree_data, "api/v1/tree")
    weather_response = post_csv(weather_data, "api/v1/weather")



    print("Sensor:", sensor_response.status_code)
    print("Tree:", tree_response.status_code)
    print("Weather:", weather_response.status_code)

    ok = all(200 <= r.status_code < 300 for r in (sensor_response, tree_response, weather_response))
    if not ok:
        print("ERROR: Some requests failed")
        print("Sensor response:", sensor_response.text)
        print("Tree response:", tree_response.text)
        print("Weather response:", weather_response.text)
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())

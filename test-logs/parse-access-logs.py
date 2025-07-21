import csv
import requests
import re

# API_URL = "https://dev.rockd.org/api/v2"
API_URL = "http://localhost:5500/usage-stats"  # Adjusted to match the API endpoint
BATCH_SIZE = 1000

def send_batch(batch):
    payload = {"data": batch}
    print(f"Sending batch of {len(batch)} rows to {API_URL}...")
    
    response = requests.post(API_URL, json=payload)
    if response.status_code == 200:
        print(f"Sent batch of {len(batch)} rows successfully.")
    else:
        print(f"Failed to send batch: {response.status_code} {response.text}")


def parse_line(line):
    # Convert to string
    line = ' '.join(line)

    # Extract IP
    ip_match = re.match(r'^(\S+)', line)
    ip = ip_match.group(1) if ip_match else None

    # Extract date
    date_match = re.search(r'\[([\d]{2}/[A-Za-z]{3}/[\d]{4})', line)
    date = date_match.group(1) if date_match else None

    # Extract lat and lng from query string
    lat_match = re.search(r'lat=([-\d.]+)', line)
    lng_match = re.search(r'lng=([-\d.]+)', line)
    lat = lat_match.group(1) if lat_match else None
    lng = lng_match.group(1) if lng_match else None

    if ip and date and lat and lng:
        return {
            "ip": ip,
            "date": date,
            "lat": lat,
            "lng": lng
        }

    return None

def main():
    batch = []
    with open('./test-logs/access.subset.log', mode='r', newline='') as file:
        reader = csv.reader(file)

        for row in reader:
            if not row or "dashboard" not in row[0]:
                continue
            row = parse_line(row)  

            if not row:
                continue

            # Prepare row dictionary (adjust keys to your API)
            record = {
                "date": row.get("date"),
                "ip": row.get("ip"),
                "lat": row.get("lat"),
                "lng": row.get("lng")
            }
            batch.append(record)

            # Send when batch size reached
            if len(batch) == BATCH_SIZE:
                print(f"Batch size reached: {len(batch)}. Sending batch...")
                send_batch(batch)
                batch = []

        # Send any leftover rows after loop
        if batch:
            send_batch(batch)

if __name__ == "__main__":
    main()
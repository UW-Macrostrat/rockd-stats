import csv
import requests

API_URL = "https://dev.rockd.org/api/v2"
BATCH_SIZE = 1000

def send_batch(batch):
    payload = {"rows": batch}
    print(f"Sending batch of {len(batch)} rows to {API_URL}...")
    
    response = requests.post(API_URL, json=payload)
    if response.status_code == 200:
        print(f"Sent batch of {len(batch)} rows successfully.")
    else:
        print(f"Failed to send batch: {response.status_code} {response.text}")

def main():
    batch = []
    with open('dashboard-logs.subset.csv', mode='r', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Skip header if present

        for row in reader:
            # Parse needed fields
            date = row[0]
            time = row[1]
            ip = row[2]
            lat = row[-1].split('=')[-1]
            lng = row[-2].split('=')[-1]

            # Prepare row dictionary (adjust keys to your API)
            record = {
                "date": date,
                "time": time,
                "ip": ip,
                "latitude": lat,
                "longitude": lng
            }
            batch.append(record)

            # Send when batch size reached
            if len(batch) == BATCH_SIZE:
                send_batch(batch)
                batch = []

        # Send any leftover rows after loop
        if batch:
            send_batch(batch)

if __name__ == "__main__":
    main()
